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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

/** Responder for checkpoint acknowledge and decline messages in the {@link Task}. */
@Internal
public interface CheckpointResponder {

    /**
     * Acknowledges the given checkpoint.
     *
     * @param jobID Job ID of the running job
     * @param executionAttemptID Execution attempt ID of the running task
     * @param checkpointId Meta data for this checkpoint
     * @param checkpointMetrics Metrics of this checkpoint
     * @param subtaskState State handles for the checkpoint
     */
    void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot subtaskState);

    /**
     * Report metrics for the given checkpoint. Can be used upon receiving abortion notification.
     *
     * @param jobID Job ID of the running job
     * @param executionAttemptID Execution attempt ID of the running task
     * @param checkpointId Meta data for this checkpoint
     * @param checkpointMetrics Metrics of this checkpoint
     */
    void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics);

    /**
     * Declines the given checkpoint.
     *
     * @param jobID Job ID of the running job
     * @param executionAttemptID Execution attempt ID of the running task
     * @param checkpointId The ID of the declined checkpoint
     * @param checkpointException The exception why the checkpoint was declined
     */
    void declineCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointException checkpointException);

    void reportInitializationMetrics(
            JobID jobId,
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics);
}
