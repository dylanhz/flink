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

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.DeserializationException;

import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Objects;

/** An internal Bitmap implementation that wraps {@link RoaringBitmap}. */
@Internal
public final class RoaringBitmap32 implements Bitmap {

    private final RoaringBitmap roaringBitmap;

    private RoaringBitmap32() {
        this.roaringBitmap = new RoaringBitmap();
    }

    private RoaringBitmap32(RoaringBitmap32 other) {
        this.roaringBitmap = other.roaringBitmap.clone();
    }

    // ~ Static Methods ----------------------------------------------------------------

    public static RoaringBitmap32 empty() {
        return new RoaringBitmap32();
    }

    public static RoaringBitmap32 from(Bitmap other) {
        if (other == null) {
            throw new NullPointerException("Bitmap cannot be created from a null Bitmap.");
        }
        return new RoaringBitmap32(toRoaringBitmap32(other));
    }

    public static RoaringBitmap32 fromBytes(byte[] bytes) {
        if (bytes == null) {
            throw new NullPointerException("Bitmap cannot be created from a null byte array.");
        }
        RoaringBitmap32 rb32 = new RoaringBitmap32();
        try {
            rb32.roaringBitmap.deserialize(ByteBuffer.wrap(bytes));
        } catch (Exception e) {
            throw new DeserializationException("Failed to deserialize bitmap from bytes.", e);
        }
        return rb32;
    }

    public static RoaringBitmap32 fromArray(int[] values) {
        if (values == null) {
            throw new NullPointerException("Bitmap cannot be created from a null int array.");
        }
        RoaringBitmap32 rb32 = new RoaringBitmap32();
        rb32.roaringBitmap.add(values);
        return rb32;
    }

    private static RoaringBitmap32 toRoaringBitmap32(Bitmap bm) {
        if (!(bm instanceof RoaringBitmap32)) {
            throw new IllegalArgumentException("Unsupported bitmap type: " + bm.getClass() + ".");
        }
        return (RoaringBitmap32) bm;
    }

    // ~ Instance Methods ---------------------------------------------------------------

    @Override
    public void add(int... values) {
        roaringBitmap.add(values);
    }

    @Override
    public void add(long rangeStart, long rangeEnd) {
        roaringBitmap.add(rangeStart, rangeEnd);
    }

    @Override
    public void and(@Nullable Bitmap other) {
        if (other == null) {
            return;
        }
        roaringBitmap.and(toRoaringBitmap32(other).roaringBitmap);
    }

    @Override
    public void andNot(@Nullable Bitmap other) {
        if (other == null) {
            return;
        }
        roaringBitmap.andNot(toRoaringBitmap32(other).roaringBitmap);
    }

    @Override
    public void clear() {
        roaringBitmap.clear();
    }

    @Override
    public boolean contains(int value) {
        return roaringBitmap.contains(value);
    }

    @Override
    public void flip(int value) {
        roaringBitmap.flip(value);
    }

    public void forEach(IntConsumer consumer) {
        roaringBitmap.forEach(consumer);
    }

    @Override
    public int getCardinality() {
        return roaringBitmap.getCardinality();
    }

    @Override
    public long getLongCardinality() {
        return roaringBitmap.getLongCardinality();
    }

    @Override
    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    @Override
    public void or(@Nullable Bitmap other) {
        if (other == null) {
            return;
        }
        roaringBitmap.or(toRoaringBitmap32(other).roaringBitmap);
    }

    @Override
    public void remove(int value) {
        roaringBitmap.remove(value);
    }

    @Override
    public int[] toArray() {
        return roaringBitmap.toArray();
    }

    @Override
    public byte[] toBytes() {
        roaringBitmap.runOptimize();
        ByteBuffer buffer = ByteBuffer.allocate(roaringBitmap.serializedSizeInBytes());
        roaringBitmap.serialize(buffer);
        return buffer.array();
    }

    @Override
    public String toString() {
        return roaringBitmap.toString();
    }

    @Override
    public void xor(@Nullable Bitmap other) {
        if (other == null) {
            return;
        }
        roaringBitmap.xor(toRoaringBitmap32(other).roaringBitmap);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RoaringBitmap32)) {
            return false;
        }
        RoaringBitmap32 other = (RoaringBitmap32) obj;
        return Objects.equals(roaringBitmap, other.roaringBitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roaringBitmap);
    }
}
