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

package org.apache.flink.types;

import org.apache.flink.types.bitmap.RoaringBitmap32;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RoaringBitmap32}. */
class RoaringBitmap32Test {

    @BeforeEach
    void setUp() {}

    @Test
    void testStaticConstructors() {
        // empty
        assertThat(RoaringBitmap32.empty().isEmpty()).isTrue();

        // from
        assertThat(RoaringBitmap32.from(RoaringBitmap32.fromArray(new int[] {1, 2})).toArray())
                .containsExactly(1, 2);
        assertThatThrownBy(() -> RoaringBitmap32.from(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Bitmap cannot be created from a null Bitmap.");

        // fromBytes
        assertThat(
                        RoaringBitmap32.fromBytes(
                                        RoaringBitmap32.fromArray(new int[] {1, 2}).toBytes())
                                .toArray())
                .containsExactly(1, 2);
        assertThatThrownBy(() -> RoaringBitmap32.fromBytes(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Bitmap cannot be created from a null byte array.");
        assertThatThrownBy(() -> RoaringBitmap32.fromBytes(new byte[0]))
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Failed to deserialize bitmap from bytes.");
        assertThatThrownBy(() -> RoaringBitmap32.fromBytes("invalid".getBytes()))
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Failed to deserialize bitmap from bytes.");

        // fromArray
        assertThat(RoaringBitmap32.fromArray(new int[0]).toArray()).containsExactly();
        assertThat(RoaringBitmap32.fromArray(new int[] {1, 2}).toArray()).containsExactly(1, 2);
        assertThatThrownBy(() -> RoaringBitmap32.fromArray(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Bitmap cannot be created from a null int array.");
    }

    @Test
    void testInstanceMethods() {
        RoaringBitmap32 rb32 = RoaringBitmap32.empty();
        assertThat(rb32.isEmpty()).isTrue();

        rb32.add(1);
        assertThat(rb32.toArray()).containsExactly(1);

        rb32.add(1, 5);
        assertThat(rb32.toArray()).containsExactly(1, 2, 3, 4);
        assertThat(rb32.getCardinality()).isEqualTo(4);

        rb32.clear();
        assertThat(rb32.isEmpty()).isTrue();

        rb32.flip(10);
        rb32.flip(11);
        rb32.flip(12);
        assertThat(rb32.contains(0)).isFalse();
        assertThat(rb32.toArray()).containsExactly(10, 11, 12);

        List<Integer> list = new ArrayList<>();
        rb32.forEach(list::add);
        assertThat(list).containsExactly(10, 11, 12);

        rb32.flip(12);
        assertThat(rb32.toArray()).containsExactly(10, 11);
        assertThat(rb32.getLongCardinality()).isEqualTo(2);

        rb32.remove(10);
        rb32.remove(12);
        assertThat(rb32.toArray()).containsExactly(11);
    }

    @Test
    void testLogicalOperations() {
        RoaringBitmap32 rb32 = RoaringBitmap32.empty();
        rb32.add(1, 5);
        assertThat(rb32.toArray()).containsExactly(1, 2, 3, 4);

        // and
        rb32.and(null);
        assertThat(rb32.toArray()).containsExactly(1, 2, 3, 4);
        rb32.and(RoaringBitmap32.fromArray(new int[] {1, 2, 6, 10}));
        assertThat(rb32.toArray()).containsExactly(1, 2);

        // or
        rb32.or(null);
        assertThat(rb32.toArray()).containsExactly(1, 2);
        rb32.or(RoaringBitmap32.fromArray(new int[] {1, 6, 10}));
        assertThat(rb32.toArray()).containsExactly(1, 2, 6, 10);

        // xor
        rb32.xor(null);
        assertThat(rb32.toArray()).containsExactly(1, 2, 6, 10);
        rb32.xor(RoaringBitmap32.fromArray(new int[] {1, 2, 3, 4, 5}));
        assertThat(rb32.toArray()).containsExactly(3, 4, 5, 6, 10);

        // andNot
        rb32.andNot(null);
        assertThat(rb32.toArray()).containsExactly(3, 4, 5, 6, 10);
        rb32.andNot(RoaringBitmap32.fromArray(new int[] {1, 2, 3, 4, 5}));
        assertThat(rb32.toArray()).containsExactly(6, 10);
    }

    @Test
    void testLargeCardinality() {
        RoaringBitmap32 rb32 = RoaringBitmap32.empty();
        rb32.add(1, Integer.MAX_VALUE);
        assertThat(rb32.getCardinality()).isEqualTo(Integer.MAX_VALUE - 1);
        assertThat(rb32.getLongCardinality()).isEqualTo(Integer.MAX_VALUE - 1);

        rb32.add(Integer.MAX_VALUE);
        assertThat(rb32.getCardinality()).isEqualTo(Integer.MAX_VALUE);
        assertThat(rb32.getLongCardinality()).isEqualTo(Integer.MAX_VALUE);

        rb32.add(-1);
        rb32.add(Integer.MIN_VALUE);
        assertThat(rb32.getCardinality()).isEqualTo((int) ((long) Integer.MAX_VALUE + 2));
        assertThat(rb32.getLongCardinality()).isEqualTo((long) Integer.MAX_VALUE + 2);
    }

    @Test
    void testOutputFormat() {
        RoaringBitmap32 rb32 = RoaringBitmap32.empty();
        assertThat(rb32.toArray()).containsExactly();
        assertThat(rb32.toString()).isEqualTo("{}");

        rb32.add(0L, 4L);
        assertThat(rb32.toArray()).containsExactly(0, 1, 2, 3);
        assertThat(rb32.toString()).isEqualTo("{0,1,2,3}");

        rb32.add(-1);
        assertThat(rb32.toArray()).containsExactly(0, 1, 2, 3, -1);
        assertThat(rb32.toString()).isEqualTo(String.format("{0,1,2,3,%d}", 0xFFFFFFFFL));

        rb32.add(Integer.MIN_VALUE);
        assertThat(rb32.toArray()).containsExactly(0, 1, 2, 3, Integer.MIN_VALUE, -1);
        assertThat(rb32.toString())
                .isEqualTo(String.format("{0,1,2,3,%d,%d}", 0x7FFFFFFFL + 1, 0xFFFFFFFFL));

        rb32.add(0L, Integer.MAX_VALUE);
        String str = rb32.toString();
        assertThat(str.substring(str.length() - 4)).isEqualTo("...}");
    }
}
