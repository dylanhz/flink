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

package org.apache.flink.types.bitmap;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A compressed data structure for storing sets of 32-bit integers.
 *
 * <p>The modifying methods in this interface modify the bitmap in place by default. Consider using
 * {@link Bitmap#from(Bitmap other)} to create a copied bitmap before modification if immutability
 * is required.
 */
@PublicEvolving
public interface Bitmap extends Serializable {

    /** Add the values to the bitmap (set the values to "true"). */
    void add(int... values);

    /** Add to the current bitmap all integers in [rangeStart,rangeEnd). */
    void add(long rangeStart, long rangeEnd);

    /**
     * Performs an in-place logical conjunction (AND) operation with another bitmap.
     *
     * <p>Does nothing if {@code other} is null.
     */
    void and(@Nullable Bitmap other);

    /**
     * Performs an in-place logical (AND-NOT) operation with another bitmap, which is equivalent to
     * {@code this AND (NOT other)}.
     *
     * <p>Does nothing if {@code other} is null.
     */
    void andNot(@Nullable Bitmap other);

    /** Reset to an empty bitmap. */
    void clear();

    /** Check whether the value appears in the bitmap. */
    boolean contains(int value);

    /** Add the value if it is not already present, otherwise remove it. */
    void flip(int value);

    /** Get the number of distinct values in the bitmap. */
    int getCardinality();

    /** Get the number of distinct values in the bitmap. This returns a full 64-bit result. */
    long getLongCardinality();

    /** Check whether the bitmap is empty. */
    boolean isEmpty();

    /**
     * Performs an in-place logical disjunction (OR) operation with another bitmap.
     *
     * <p>Does nothing if {@code other} is null.
     */
    void or(@Nullable Bitmap other);

    /** Remove the value from the bitmap (set the value to "false"). */
    void remove(int value);

    /**
     * Get the values in the bitmap as an array, the values are sorted by {@link
     * Integer#compareUnsigned}. Throws error if the cardinality is too large.
     */
    int[] toArray();

    /**
     * Get the standard serialized bytes of the bitmap.
     *
     * @see <a href="https://github.com/RoaringBitmap/RoaringFormatSpec">Roaring Format
     *     Specification</a>
     */
    byte[] toBytes();

    /**
     * Get the string representation of the bitmap, the values are sorted by {@link
     * Integer#compareUnsigned}. The string may be truncated if it is too long, and will end with
     * "...".
     *
     * <p>For example:
     *
     * <ul>
     *   <li>{@code "{}"}, {@code "{1,2,3,4,5}"}
     *   <li>Negative values (converted to unsigned): {@code "{0,1,4294967294,4294967295}"}
     *   <li>String too long: {@code "{1,2,3...}"}
     * </ul>
     */
    String toString();

    /**
     * Performs an in-place symmetric difference (XOR) operation with another bitmap.
     *
     * <p>Does nothing if {@code other} is null.
     */
    void xor(@Nullable Bitmap other);

    // ~ Static Methods --------------------------------------------------------------

    /** Get an empty bitmap. */
    static Bitmap empty() {
        return RoaringBitmap32.empty();
    }

    /**
     * Get a copied bitmap.
     *
     * @throws NullPointerException if {@code other} is null
     */
    static Bitmap from(Bitmap other) {
        return RoaringBitmap32.from(other);
    }

    /**
     * Get a bitmap from standard serialized bytes.
     *
     * @throws NullPointerException if {@code bytes} is null
     * @throws org.apache.flink.types.DeserializationException if failed to deserialize bitmap from
     *     bytes
     * @see <a href="https://github.com/RoaringBitmap/RoaringFormatSpec">Roaring Format
     *     Specification</a>
     */
    static Bitmap fromBytes(byte[] bytes) {
        return RoaringBitmap32.fromBytes(bytes);
    }

    /**
     * Get a bitmap from an array of values.
     *
     * @throws NullPointerException if {@code values} is null
     */
    static Bitmap fromArray(int[] values) {
        return RoaringBitmap32.fromArray(values);
    }
}
