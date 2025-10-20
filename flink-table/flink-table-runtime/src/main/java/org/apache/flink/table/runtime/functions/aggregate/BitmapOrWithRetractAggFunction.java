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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.bitmap.Bitmap;
import org.apache.flink.types.bitmap.RoaringBitmap32;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Built-in BITMAP_OR_AGG with retraction aggregate function. */
@Internal
public final class BitmapOrWithRetractAggFunction
        extends BuiltInAggregateFunction<
                Bitmap, BitmapOrWithRetractAggFunction.BitmapOrWithRetractAccumulator> {

    private final transient DataType valueDataType;

    public BitmapOrWithRetractAggFunction(LogicalType valueType) {
        this.valueDataType = toInternalDataType(valueType);
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Collections.singletonList(valueDataType);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                BitmapOrWithRetractAccumulator.class,
                DataTypes.FIELD(
                        "valueCount",
                        MapView.newMapViewDataType(
                                        DataTypes.INT().notNull(), DataTypes.INT().notNull())
                                .notNull()));
    }

    @Override
    public DataType getOutputDataType() {
        return DataTypes.BITMAP().notNull();
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    public static class BitmapOrWithRetractAccumulator {

        public MapView<Integer, Integer> valueCount = new MapView<>();

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            BitmapOrWithRetractAccumulator that = (BitmapOrWithRetractAccumulator) obj;
            return Objects.equals(valueCount, that.valueCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(valueCount);
        }
    }

    @Override
    public BitmapOrWithRetractAccumulator createAccumulator() {
        return new BitmapOrWithRetractAccumulator();
    }

    public void resetAccumulator(BitmapOrWithRetractAccumulator acc) {
        acc.valueCount.clear();
    }

    @Override
    public Bitmap getValue(BitmapOrWithRetractAccumulator acc) {
        // TODO: add in batch
        Bitmap bitmap = Bitmap.empty();
        try {
            for (Map.Entry<Integer, Integer> entry : acc.valueCount.entries()) {
                if (entry.getValue() > 0) {
                    bitmap.add(entry.getKey());
                }
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }

        return bitmap;
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    public void accumulate(BitmapOrWithRetractAccumulator acc, @Nullable Bitmap bitmap)
            throws Exception {
        if (bitmap != null) {
            RoaringBitmap32 rbm32 = (RoaringBitmap32) bitmap;
            rbm32.forEach(
                    value -> {
                        try {
                            Integer count = acc.valueCount.get(value);
                            count = count == null ? 1 : count + 1;
                            if (count == 0) {
                                acc.valueCount.remove(value);
                            } else {
                                acc.valueCount.put(value, count);
                            }
                        } catch (Exception e) {
                            throw new FlinkRuntimeException(e);
                        }
                    });
        }
    }

    public void retract(BitmapOrWithRetractAccumulator acc, @Nullable Bitmap bitmap)
            throws Exception {
        if (bitmap != null) {
            RoaringBitmap32 rbm32 = (RoaringBitmap32) bitmap;
            rbm32.forEach(
                    value -> {
                        try {
                            Integer count = acc.valueCount.get(value);
                            count = count == null ? -1 : count - 1;
                            if (count == 0) {
                                acc.valueCount.remove(value);
                            } else {
                                acc.valueCount.put(value, count);
                            }
                        } catch (Exception e) {
                            throw new FlinkRuntimeException(e);
                        }
                    });
        }
    }

    public void merge(
            BitmapOrWithRetractAccumulator acc, Iterable<BitmapOrWithRetractAccumulator> its)
            throws Exception {
        for (BitmapOrWithRetractAccumulator other : its) {
            for (Map.Entry<Integer, Integer> entry : other.valueCount.entries()) {
                Integer value = entry.getKey();
                Integer count = entry.getValue();
                Integer curCount = acc.valueCount.get(value);
                curCount = curCount == null ? count : curCount + count;

                if (curCount == 0) {
                    acc.valueCount.remove(value);
                } else {
                    acc.valueCount.put(value, curCount);
                }
            }
        }
    }
}
