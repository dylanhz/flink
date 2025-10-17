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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.bitmap.Bitmap;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;

/** Test bitmap functions correct behaviour. */
class BitmapFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                        bitmapBuildTestCases(),
                        bitmapCardinalityTestCases(),
                        bitmapLongCardinalityTestCases(),
                        bitmapToArrayTestCases(),
                        bitmapToStringTestCases())
                .flatMap(s -> s);
    }

    private Stream<TestSetSpec> bitmapBuildTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_BUILD)
                        .onFieldsWithData(
                                null,
                                new Integer[] {1, null, 1},
                                new Integer[] {-1},
                                new Integer[] {1, 2, 3, -4})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()).notNull())
                        // null array
                        .testResult(
                                $("f0").bitmapBuild(), "BITMAP_BUILD(f0)", null, DataTypes.BITMAP())
                        // null array element
                        .testResult(
                                $("f1").arrayRemove(1).bitmapBuild(),
                                "BITMAP_BUILD(ARRAY_REMOVE(f1, 1))",
                                Bitmap.empty(),
                                DataTypes.BITMAP())
                        .testResult(
                                $("f1").bitmapBuild(),
                                "BITMAP_BUILD(f1)",
                                Bitmap.fromArray(new int[] {1}),
                                DataTypes.BITMAP())
                        // empty array
                        .testResult(
                                $("f2").arrayRemove(-1).bitmapBuild(),
                                "BITMAP_BUILD(ARRAY_REMOVE(f2, -1))",
                                Bitmap.empty(),
                                DataTypes.BITMAP())
                        // normal cases
                        .testResult(
                                $("f2").bitmapBuild(),
                                "BITMAP_BUILD(f2)",
                                Bitmap.fromArray(new int[] {-1}),
                                DataTypes.BITMAP())
                        .testResult(
                                $("f3").bitmapBuild(),
                                "BITMAP_BUILD(f3)",
                                Bitmap.fromArray(new int[] {1, 2, 3, -4}),
                                DataTypes.BITMAP().notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_BUILD, "Validation Error")
                        .onFieldsWithData(1024, new long[] {1L, 2L})
                        .andDataTypes(DataTypes.INT(), DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testTableApiValidationError(
                                $("f0").bitmapBuild(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_BUILD(array ARRAY<INT> NOT NULL)\n"
                                        + "BITMAP_BUILD(array ARRAY<INT>)")
                        .testSqlValidationError(
                                "BITMAP_BUILD(f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_BUILD(array ARRAY<INT> NOT NULL)\n"
                                        + "BITMAP_BUILD(array ARRAY<INT>)"));
    }

    private Stream<TestSetSpec> bitmapCardinalityTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_CARDINALITY)
                        .onFieldsWithData(new Integer[] {-1}, new Integer[] {1, 2, 3, -4})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()).notNull())
                        // TODO null test
                        // empty
                        .testResult(
                                $("f0").arrayRemove(-1).bitmapBuild().bitmapCardinality(),
                                "BITMAP_CARDINALITY(BITMAP_BUILD(ARRAY_REMOVE(f0, -1)))",
                                0,
                                DataTypes.INT())
                        // normal cases
                        .testResult(
                                $("f0").bitmapBuild().bitmapCardinality(),
                                "BITMAP_CARDINALITY(BITMAP_BUILD(f0))",
                                1,
                                DataTypes.INT())
                        .testResult(
                                $("f1").bitmapBuild().bitmapCardinality(),
                                "BITMAP_CARDINALITY(BITMAP_BUILD(f1))",
                                4,
                                DataTypes.INT().notNull()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.BITMAP_CARDINALITY, "Validation Error")
                        .onFieldsWithData(1024, new int[] {1, 2})
                        .andDataTypes(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiValidationError(
                                $("f0").bitmapCardinality(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_CARDINALITY(bitmap <BITMAP>)")
                        .testSqlValidationError(
                                "BITMAP_CARDINALITY(f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_CARDINALITY(bitmap <BITMAP>)"));
    }

    private Stream<TestSetSpec> bitmapLongCardinalityTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_LONG_CARDINALITY)
                        .onFieldsWithData(new Integer[] {-1}, new Integer[] {1, 2, 3, -4})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()).notNull())
                        // TODO null test
                        // empty
                        .testResult(
                                $("f0").arrayRemove(-1).bitmapBuild().bitmapLongCardinality(),
                                "BITMAP_LONG_CARDINALITY(BITMAP_BUILD(ARRAY_REMOVE(f0, -1)))",
                                0L,
                                DataTypes.BIGINT())
                        // normal cases
                        .testResult(
                                $("f0").bitmapBuild().bitmapLongCardinality(),
                                "BITMAP_LONG_CARDINALITY(BITMAP_BUILD(f0))",
                                1L,
                                DataTypes.BIGINT())
                        .testResult(
                                $("f1").bitmapBuild().bitmapLongCardinality(),
                                "BITMAP_LONG_CARDINALITY(BITMAP_BUILD(f1))",
                                4L,
                                DataTypes.BIGINT().notNull()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.BITMAP_LONG_CARDINALITY,
                                "Validation Error")
                        .onFieldsWithData(1024, new int[] {1, 2})
                        .andDataTypes(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiValidationError(
                                $("f0").bitmapLongCardinality(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_LONG_CARDINALITY(bitmap <BITMAP>)")
                        .testSqlValidationError(
                                "BITMAP_LONG_CARDINALITY(f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_LONG_CARDINALITY(bitmap <BITMAP>)"));
    }

    private Stream<TestSetSpec> bitmapToArrayTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_TO_ARRAY)
                        .onFieldsWithData(
                                new Integer[] {-1}, new Integer[] {Integer.MIN_VALUE, -1, 1, 2, 3})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()).notNull())
                        // TODO null test
                        // empty
                        .testResult(
                                $("f0").arrayRemove(-1).bitmapBuild().bitmapToArray(),
                                "BITMAP_TO_ARRAY(BITMAP_BUILD(ARRAY_REMOVE(f0, -1)))",
                                new Integer[0],
                                DataTypes.ARRAY(DataTypes.INT()))
                        // normal cases
                        .testResult(
                                $("f0").bitmapBuild().bitmapToArray(),
                                "BITMAP_TO_ARRAY(BITMAP_BUILD(f0))",
                                new Integer[] {-1},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f1").bitmapBuild().bitmapToArray(),
                                "BITMAP_TO_ARRAY(BITMAP_BUILD(f1))",
                                new Integer[] {1, 2, 3, Integer.MIN_VALUE, -1},
                                DataTypes.ARRAY(DataTypes.INT()).notNull()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.BITMAP_TO_ARRAY, "Validation Error")
                        .onFieldsWithData(1024, new int[] {1, 2})
                        .andDataTypes(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiValidationError(
                                $("f0").bitmapToArray(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_TO_ARRAY(bitmap <BITMAP>)")
                        .testSqlValidationError(
                                "BITMAP_TO_ARRAY(f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_TO_ARRAY(bitmap <BITMAP>)"));
    }

    private Stream<TestSetSpec> bitmapToStringTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_TO_STRING)
                        .onFieldsWithData(
                                new Integer[] {-1}, new Integer[] {Integer.MIN_VALUE, -1, 1, 2, 3})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()).notNull())
                        // TODO null test
                        // empty
                        .testResult(
                                $("f0").arrayRemove(-1).bitmapBuild().bitmapToString(),
                                "BITMAP_TO_STRING(BITMAP_BUILD(ARRAY_REMOVE(f0, -1)))",
                                "{}",
                                DataTypes.STRING())
                        // normal cases
                        .testResult(
                                $("f0").bitmapBuild().bitmapToString(),
                                "BITMAP_TO_STRING(BITMAP_BUILD(f0))",
                                String.format("{%s}", 0xFFFFFFFFL),
                                DataTypes.STRING())
                        .testResult(
                                $("f1").bitmapBuild().bitmapToString(),
                                "BITMAP_TO_STRING(BITMAP_BUILD(f1))",
                                String.format("{1,2,3,%s,%s}", 0x80000000L, 0xFFFFFFFFL),
                                DataTypes.STRING().notNull()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.BITMAP_TO_STRING, "Validation Error")
                        .onFieldsWithData(1024, new int[] {1, 2})
                        .andDataTypes(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiValidationError(
                                $("f0").bitmapToString(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_TO_STRING(bitmap <BITMAP>)")
                        .testSqlValidationError(
                                "BITMAP_TO_STRING(f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_TO_STRING(bitmap <BITMAP>)"));
    }
}
