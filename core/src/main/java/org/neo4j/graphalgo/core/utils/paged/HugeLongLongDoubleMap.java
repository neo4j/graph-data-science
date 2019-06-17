/*
 * Copyright (c) 2017-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.hppc.BitMixer;
import com.carrotsearch.hppc.Containers;
import org.neo4j.graphalgo.core.utils.BitUtil;

import java.util.concurrent.atomic.AtomicLong;

/**
 * map with two longs as keys and huge underlying storage, so it can
 * store more than 2B values
 */
public final class HugeLongLongDoubleMap {

    private final AllocationTracker tracker;

    private HugeLongArray keys1;
    private HugeLongArray keys2;
    private HugeDoubleArray values;
    private HugeCursor<long[]> keysCursor;

    private int keyMixer;
    private long assigned;
    private long mask;
    private long resizeAt;

    private final static long DEFAULT_EXPECTED_ELEMENTS = 4L;
    private final static double LOAD_FACTOR = 0.75;

    /**
     * New instance with sane defaults.
     */
    public HugeLongLongDoubleMap(AllocationTracker tracker) {
        this(DEFAULT_EXPECTED_ELEMENTS, tracker);
    }

    /**
     * New instance with sane defaults.
     */
    public HugeLongLongDoubleMap(long expectedElements, AllocationTracker tracker) {
        this.tracker = tracker;
        initialBuffers(expectedElements);
    }

    public void addTo(long key1, long key2, double value) {
        addTo0(1L + key1, 1L + key2, value);
    }

    public double getOrDefault(long key1, long key2, double defaultValue) {
        return getOrDefault0(1L + key1, 1L + key2, defaultValue);
    }

    private void addTo0(long key1, long key2, double value) {
        assert assigned < mask + 1L;
        final long key = hashKey(key1, key2);

        long slot = findSlot(key1, key2, key & mask);
        assert slot != -1L;
        if (slot >= 0L) {
            values.addTo(slot, value);
            return;
        }

        slot = ~(1L + slot);
        if (assigned == resizeAt) {
            allocateThenInsertThenRehash(slot, key1, key2, value);
        } else {
            keys1.set(slot, key1);
            keys2.set(slot, key2);
            values.set(slot, value);
        }

        assigned++;
    }

    private double getOrDefault0(long key1, long key2, double defaultValue) {
        final long key = hashKey(key1, key2);

        long slot = findSlot(key1, key2, key & mask);
        if (slot >= 0L) {
            return values.get(slot);
        }

        return defaultValue;
    }

    private long findSlot(
            long key1,
            long key2,
            long start) {
        HugeLongArray keys1 = this.keys1;
        HugeLongArray keys2 = this.keys2;
        HugeCursor<long[]> cursor = this.keysCursor;
        long slot = findSlot(key1, key2, start, keys1.size(), keys1, keys2, cursor);
        if (slot == -1L) {
            slot = findSlot(key1, key2, 0L, start, keys1, keys2, cursor);
        }
        return slot;
    }

    private long findSlot(
            long key1,
            long key2,
            long start,
            long end,
            HugeLongArray keys1,
            HugeLongArray keys2,
            HugeCursor<long[]> cursor) {

        long slot = start;
        int blockPos, blockEnd;
        long[] keysBlock;
        long existing;
        keys1.cursor(cursor, start, end);
        while (cursor.next()) {
            keysBlock = cursor.array;
            blockPos = cursor.offset;
            blockEnd = cursor.limit;
            while (blockPos < blockEnd) {
                existing = keysBlock[blockPos];
                if (existing == key1 && keys2.get(slot) == key2) {
                    return slot;
                }
                if (existing == 0L) {
                    return ~slot - 1L;
                }
                ++blockPos;
                ++slot;
            }
        }
        return -1L;
    }

    public long size() {
        return assigned;
    }

    public boolean isEmpty() {
        return size() == 0L;
    }

    public void release() {
        long released = 0L;
        released += keys1.release();
        released += keys2.release();
        released += values.release();
        tracker.remove(released);

        keys1 = null;
        keys2 = null;
        values = null;
        assigned = 0L;
        mask = 0L;
    }

    private void initialBuffers(long expectedElements) {
        allocateBuffers(minBufferSize(expectedElements), tracker);
    }

    /**
     * Convert the contents of this map to a human-friendly string.
     */
    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        buffer.append('[');

        HugeCursor<long[]> keys1 = this.keys1.cursor(this.keys1.newCursor());
        HugeCursor<long[]> keys2 = this.keys2.cursor(this.keys2.newCursor());
        HugeCursor<double[]> values = this.values.cursor(this.values.newCursor());

        long key1;
        while (keys1.next()) {
            keys2.next();
            values.next();

            long[] ks1 = keys1.array;
            long[] ks2 = keys2.array;
            double[] vs = values.array;
            int end = keys1.limit;
            for (int pos = keys1.offset; pos < end; ++pos) {
                if ((key1 = ks1[pos]) != 0L) {
                    buffer
                            .append('(')
                            .append(key1 - 1L)
                            .append(',')
                            .append(ks2[pos] - 1L)
                            .append(")=>")
                            .append(vs[pos])
                            .append(", ");
                }
            }
        }

        if (buffer.length() > 1) {
            buffer.setLength(buffer.length() - 1);
            buffer.setCharAt(buffer.length() - 1, ']');
        } else {
            buffer.append(']');
        }

        return buffer.toString();
    }

    private long hashKey(long key1, long key2) {
        return BitMixer.mix64(key1 ^ key2 ^ (long) this.keyMixer);
    }

    /**
     * Allocate new internal buffers. This method attempts to allocate
     * and assign internal buffers atomically (either allocations succeed or not).
     */
    private void allocateBuffers(long arraySize, AllocationTracker tracker) {
        assert BitUtil.isPowerOfTwo(arraySize);

        // Compute new hash mixer candidate before expanding.
        final int newKeyMixer = RandomSeed.next();

        // Ensure no change is done if we hit an OOM.
        HugeLongArray prevKeys1 = this.keys1;
        HugeLongArray prevKeys2 = this.keys2;
        HugeDoubleArray prevValues = this.values;
        try {
            this.keys1 = HugeLongArray.newArray(arraySize, tracker);
            this.keys2 = HugeLongArray.newArray(arraySize, tracker);
            this.values = HugeDoubleArray.newArray(arraySize, tracker);
            keysCursor = keys1.newCursor();
        } catch (OutOfMemoryError e) {
            this.keys1 = prevKeys1;
            this.keys2 = prevKeys2;
            this.values = prevValues;
            throw e;
        }

        this.resizeAt = expandAtCount(arraySize);
        this.keyMixer = newKeyMixer;
        this.mask = arraySize - 1L;
    }

    /**
     * Rehash from old buffers to new buffers.
     */
    private void rehash(
            HugeLongArray fromKeys1,
            HugeLongArray fromKeys2,
            HugeDoubleArray fromValues) {
        assert fromKeys1.size() == fromValues.size() &&
                fromKeys2.size() == fromValues.size() &&
                BitUtil.isPowerOfTwo(fromValues.size());

        // Rehash all stored key/value pairs into the new buffers.
        final HugeLongArray newKeys1 = this.keys1;
        final HugeLongArray newKeys2 = this.keys2;
        final HugeDoubleArray newValues = this.values;
        final long mask = this.mask;

        HugeCursor<long[]> keys1 = fromKeys1.cursor(fromKeys1.newCursor());
        HugeCursor<long[]> keys2 = fromKeys2.cursor(fromKeys2.newCursor());
        HugeCursor<double[]> values = fromValues.cursor(fromValues.newCursor());

        long key1, key2, slot;
        while (keys1.next()) {
            keys2.next();
            values.next();

            long[] ks1 = keys1.array;
            long[] ks2 = keys2.array;
            double[] vs = values.array;
            int end = keys1.limit;
            for (int pos = keys1.offset; pos < end; ++pos) {
                if ((key1 = ks1[pos]) != 0L) {
                    key2 = ks2[pos];
                    slot = hashKey(key1, key2) & mask;
                    slot = findSlot(key1, key2, slot);
                    slot = ~(1L + slot);
                    newKeys1.set(slot, key1);
                    newKeys2.set(slot, key2);
                    newValues.set(slot, vs[pos]);
                }
            }
        }
    }


    /**
     * This method is invoked when there is a new key/ value pair to be inserted into
     * the buffers but there is not enough empty slots to do so.
     *
     * New buffers are allocated. If this succeeds, we know we can proceed
     * with rehashing so we assign the pending element to the previous buffer
     * and rehash all keys, substituting new buffers at the end.
     */
    private void allocateThenInsertThenRehash(long slot, long pendingKey1, long pendingKey2, double pendingValue) {
        assert assigned == resizeAt;

        // Try to allocate new buffers first. If we OOM, we leave in a consistent state.
        final HugeLongArray prevKeys1 = this.keys1;
        final HugeLongArray prevKeys2 = this.keys2;
        final HugeDoubleArray prevValues = this.values;
        allocateBuffers(nextBufferSize(mask + 1), tracker);
        assert this.keys1.size() > prevKeys1.size();

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the old arrays before rehashing.
        prevKeys1.set(slot, pendingKey1);
        prevKeys2.set(slot, pendingKey2);
        prevValues.set(slot, pendingValue);

        // Rehash old keys, including the pending key.
        rehash(prevKeys1, prevKeys2, prevValues);

        long released = 0L;
        released += prevKeys1.release();
        released += prevKeys2.release();
        released += prevValues.release();
        tracker.remove(released);
    }


    private final static int MIN_HASH_ARRAY_LENGTH = 4;

    private static long minBufferSize(long elements) {
        if (elements < 0L) {
            throw new IllegalArgumentException(
                    "Number of elements must be >= 0: " + elements);
        }

        long length = (long) Math.ceil((double) elements / LOAD_FACTOR);
        if (length == elements) {
            length++;
        }
        length = Math.max(MIN_HASH_ARRAY_LENGTH, BitUtil.nextHighestPowerOfTwo(length));
        return length;
    }

    private static long nextBufferSize(long arraySize) {
        assert BitUtil.isPowerOfTwo(arraySize);
        return arraySize << 1;
    }

    private static long expandAtCount(long arraySize) {
        assert BitUtil.isPowerOfTwo(arraySize);
        return Math.min(arraySize, (long) Math.ceil(arraySize * LOAD_FACTOR));
    }

    private static final class RandomSeed {
        private static final RandomSeed INSTANCE = new RandomSeed();

        private static int next() {
            return INSTANCE.newSeed();
        }

        private final AtomicLong seed;

        private RandomSeed() {
            seed = new AtomicLong(Containers.randomSeed64());
        }

        private int newSeed() {
            return (int) BitMixer.mix64(seed.incrementAndGet());
        }
    }
}
