/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.util.WrappingRuntimeException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of a {@link InternalTimeServiceManager} that manages timers with a single
 * active key at a time. Can be used in a BATCH execution mode.
 */
public class BatchExecutionInternalTimeServiceManager<K>
        implements InternalTimeServiceManager<K>, KeyedStateBackend.KeySelectionListener<K> {

    private final ProcessingTimeService processingTimeService;
    private final Map<String, BatchExecutionInternalTimeService<K, ?>> timerServices =
            new HashMap<>();

    // In batch mode, there is a chance that the operator is {@link AsyncKeyOrderedProcessing} and
    // we
    // should perform correctly when the timer fires.
    private final boolean asyncStateProcessingMode;

    public BatchExecutionInternalTimeServiceManager(
            ProcessingTimeService processingTimeService, boolean asyncStateProcessingMode) {
        this.processingTimeService = checkNotNull(processingTimeService);
        this.asyncStateProcessingMode = asyncStateProcessingMode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <N> InternalTimerService<N> getInternalTimerService(
            String name,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            Triggerable<K, N> triggerable) {
        BatchExecutionInternalTimeService<K, N> timerService =
                (BatchExecutionInternalTimeService<K, N>) timerServices.get(name);
        if (timerService == null) {
            timerService =
                    asyncStateProcessingMode
                            ? new BatchExecutionInternalTimeServiceWithAsyncState<>(
                                    processingTimeService, triggerable)
                            : new BatchExecutionInternalTimeService<>(
                                    processingTimeService, triggerable);
            timerServices.put(name, timerService);
        }

        return timerService;
    }

    @Override
    public void advanceWatermark(Watermark watermark) {
        if (watermark.getTimestamp() == Long.MAX_VALUE) {
            keySelected(null);
        }
    }

    @Override
    public boolean tryAdvanceWatermark(
            Watermark watermark, ShouldStopAdvancingFn shouldStopAdvancingFn) {
        advanceWatermark(watermark);
        return true;
    }

    @Override
    public void snapshotToRawKeyedState(
            KeyedStateCheckpointOutputStream context, String operatorName) throws Exception {
        throw new UnsupportedOperationException("Checkpoints are not supported in BATCH execution");
    }

    @SuppressWarnings("unchecked")
    public static <K> InternalTimeServiceManager<K> create(
            TaskIOMetricGroup taskIOMetricGroup,
            PriorityQueueSetFactory factory,
            KeyGroupRange keyGroupRange,
            ClassLoader userClassloader,
            KeyContext keyContext, // the operator
            ProcessingTimeService processingTimeService,
            Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates,
            StreamTaskCancellationContext cancellationContext) {
        BatchExecutionKeyedStateBackend<K> theFactory = null;
        boolean asyncStateProcessingMode = false;
        if (factory instanceof BatchExecutionKeyedStateBackend) {
            theFactory = (BatchExecutionKeyedStateBackend<K>) factory;
        } else if (factory instanceof AsyncKeyedStateBackendAdaptor) {
            KeyedStateBackend<K> keyedStateBackend =
                    ((AsyncKeyedStateBackendAdaptor<K>) factory).getKeyedStateBackend();
            if (keyedStateBackend instanceof BatchExecutionKeyedStateBackend) {
                theFactory = (BatchExecutionKeyedStateBackend<K>) keyedStateBackend;
                asyncStateProcessingMode = true;
            }
        }
        if (theFactory == null) {
            throw new IllegalStateException(
                    "Batch execution specific time service can work only with BatchExecutionKeyedStateBackend");
        }

        BatchExecutionInternalTimeServiceManager<K> timeServiceManager =
                new BatchExecutionInternalTimeServiceManager<>(
                        processingTimeService, asyncStateProcessingMode);
        theFactory.registerKeySelectionListener(timeServiceManager);
        return timeServiceManager;
    }

    @Override
    public void keySelected(K newKey) {
        try {
            for (BatchExecutionInternalTimeService<K, ?> value : timerServices.values()) {
                value.setCurrentKey(newKey);
            }
        } catch (Exception e) {
            throw new WrappingRuntimeException(e);
        }
    }
}
