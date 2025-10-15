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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.types.bitmap.Bitmap;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link BuiltInFunctionDefinitions#BITMAP_BUILD}. */
@Internal
public class BitmapBuildFunction extends BuiltInScalarFunction {

    public BitmapBuildFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.BITMAP_BUILD, context);
    }

    public Bitmap eval(@Nullable ArrayData array) {
        if (array == null) {
            return null;
        }

        // TODO: benchmark
        // adding all values in one Bitmap#add(int... values) call is more efficient
        Bitmap bm = Bitmap.empty();
        List<Integer> nonNullArray = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            if (!array.isNullAt(i)) {
                nonNullArray.add(array.getInt(i));
            }
        }
        bm.add(nonNullArray.stream().mapToInt(Integer::intValue).toArray());

        return bm;
    }
}
