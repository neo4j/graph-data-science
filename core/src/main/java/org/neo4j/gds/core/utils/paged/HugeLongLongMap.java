/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.core.utils.paged;

import com.carrotsearch.hppc.BitMixer;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.mem.BitUtil;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * map with {@code long=>long} mapping and huge underlying storage, so it can
 * store more than 2B values
 */
public final class HugeLongLongMap implements Iterable<LongLongCursor> {

    private static final MemoryEstimation MEMORY_REQUIREMENTS = MemoryEstimations
            .builder(HugeLongLongMap.class)
            .field("keysCursor", HugeCursor.class)
            .field("entries", EntryIterator.class)
            .perNode("keys", HugeLongArray::memoryEstimation)
            .perNode("values", HugeLongArray::memoryEstimation)
            .build();

    private HugeLongArray keys;
    private HugeLongArray values;
    private HugeCursor<long[]> keysCursor;
    private EntryIterator entries;

    private long assigned;
    private long mask;
    private long resizeAt;

    private static final long DEFAULT_EXPECTED_ELEMENTS = 4L;
    private static final double LOAD_FACTOR = 0.75;

    public static MemoryEstimation memoryEstimation() {
        return MEMORY_REQUIREMENTS;
    }

    /**
     * New instance with sane defaults.
     */
    public HugeLongLongMap() {
        this(DEFAULT_EXPECTED_ELEMENTS);
    }

    /**
     * New instance with sane defaults.
     */
    public HugeLongLongMap(long expectedElements) {
        initialBuffers(expectedElements);
    }

    public long sizeOf() {
        return keys.sizeOf() + values.sizeOf();
    }

    public void put(long key, long value) {
        put0(1L + key, value);
    }

    public void addTo(long key, long value) {
        addTo0(1L + key, value);
    }

    public long getOrDefault(long key, long defaultValue) {
        return getOrDefault0(1L + key, defaultValue);
    }

    public boolean containsKey(long key) {
        return containsKey0(1L + key);
    }

    private boolean containsKey0(long key) {
        final long hash = BitMixer.mixPhi(key);
        return findSlot(key, hash & mask) >= 0L;
    }

    private void put0(long key, long value) {
        assert assigned < mask + 1L;
        final long hash = BitMixer.mixPhi(key);
        long slot = findSlot(key, hash & mask);
        assert slot != -1L;
        if (slot >= 0L) {
            values.set(slot, value);
            return;
        }

        slot = ~(1L + slot);
        if (assigned == resizeAt) {
            allocateThenInsertThenRehash(slot, key, value);
        } else {
            values.set(slot, value);
            keys.set(slot, key);
        }

        assigned++;
    }

    private void addTo0(long key, long value) {
        assert assigned < mask + 1L;
        final long hash = BitMixer.mixPhi(key);
        long slot = findSlot(key, hash & mask);
        assert slot != -1L;
        if (slot >= 0L) {
            values.addTo(slot, value);
            return;
        }

        slot = ~(1L + slot);
        if (assigned == resizeAt) {
            allocateThenInsertThenRehash(slot, key, value);
        } else {
            values.set(slot, value);
            keys.set(slot, key);
        }

        assigned++;
    }

    private long getOrDefault0(long key, long defaultValue) {
        final long hash = BitMixer.mixPhi(key);
        long slot = findSlot(key, hash & mask);
        if (slot >= 0L) {
            return values.get(slot);
        }

        return defaultValue;
    }

    private long findSlot(
            long key,
            long start) {
        HugeLongArray keys = this.keys;
        HugeCursor<long[]> cursor = this.keysCursor;
        long slot = findSlot(key, start, keys.size(), keys, cursor);
        if (slot == -1L) {
            slot = findSlot(key, 0L, start, keys, cursor);
        }
        return slot;
    }

    private long findSlot(
            long key,
            long start,
            long end,
            HugeLongArray keys,
            HugeCursor<long[]> cursor) {

        long slot = start;
        int blockPos, blockEnd;
        long[] keysBlock;
        long existing;
        keys.initCursor(cursor, start, end);
        while (cursor.next()) {
            keysBlock = cursor.array;
            blockPos = cursor.offset;
            blockEnd = cursor.limit;
            while (blockPos < blockEnd) {
                existing = keysBlock[blockPos];
                if (existing == key) {
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

    public void clear() {
        assigned = 0L;

        keys.fill(0);
        values.fill(0);
    }

    public void release() {
        keys.release();
        values.release();

        keys = null;
        values = null;
        assigned = 0L;
        mask = 0L;
    }

    private void initialBuffers(long expectedElements) {
        allocateBuffers(minBufferSize(expectedElements));
    }

    @Override
    public Iterator<LongLongCursor> iterator() {
        return entries.reset();
    }

    /**
     * Convert the contents of this map to a human-friendly string.
     */
    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        buffer.append('[');

        for (LongLongCursor cursor : this) {
            buffer
                    .append(cursor.key)
                    .append("=>")
                    .append(cursor.value)
                    .append(", ");
        }

        if (buffer.length() > 1) {
            buffer.setLength(buffer.length() - 1);
            buffer.setCharAt(buffer.length() - 1, ']');
        } else {
            buffer.append(']');
        }

        return buffer.toString();
    }

    /**
     * Allocate new internal buffers. This method attempts to allocate
     * and assign internal buffers atomically (either allocations succeed or not).
     */
    private void allocateBuffers(long arraySize) {
        assert BitUtil.isPowerOfTwo(arraySize);

        // Ensure no change is done if we hit an OOM.
        HugeLongArray prevKeys = this.keys;
        HugeLongArray prevValues = this.values;
        try {
            this.keys = HugeLongArray.newArray(arraySize);
            this.values = HugeLongArray.newArray(arraySize);
            keysCursor = keys.newCursor();
            entries = new EntryIterator();
        } catch (OutOfMemoryError e) {
            this.keys = prevKeys;
            this.values = prevValues;
            throw e;
        }

        this.resizeAt = expandAtCount(arraySize);
        this.mask = arraySize - 1L;
    }

    /**
     * Rehash from old buffers to new buffers.
     */
    private void rehash(
            HugeLongArray fromKeys,
            HugeLongArray fromValues) {
        assert fromKeys.size() == fromValues.size() &&
                BitUtil.isPowerOfTwo(fromValues.size());

        // Rehash all stored key/value pairs into the new buffers.
        final HugeLongArray newKeys = this.keys;
        final HugeLongArray newValues = this.values;
        final long mask = this.mask;

        try (EntryIterator fromEntries = new EntryIterator(fromKeys, fromValues)) {
            for (LongLongCursor cursor : fromEntries) {
                long key = cursor.key + 1L;
                long slot = BitMixer.mixPhi(key) & mask;
                slot = findSlot(key, slot);
                slot = ~(1L + slot);
                newKeys.set(slot, key);
                newValues.set(slot, cursor.value);
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
    private void allocateThenInsertThenRehash(long slot, long pendingKey, long pendingValue) {
        assert assigned == resizeAt;

        // Try to allocate new buffers first. If we OOM, we leave in a consistent state.
        final HugeLongArray prevKeys = this.keys;
        final HugeLongArray prevValues = this.values;
        allocateBuffers(nextBufferSize(mask + 1));
        assert this.keys.size() > prevKeys.size();

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the old arrays before rehashing.
        prevKeys.set(slot, pendingKey);
        prevValues.set(slot, pendingValue);

        // Rehash old keys, including the pending key.
        rehash(prevKeys, prevValues);

        prevKeys.release();
        prevValues.release();
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

    private final class EntryIterator implements AutoCloseable, Iterable<LongLongCursor>, Iterator<LongLongCursor>  {
        private HugeCursor<long[]> keyCursor;
        private HugeCursor<long[]> valueCursor;
        private boolean nextFetched = false;
        private boolean hasNext = false;
        private LongLongCursor cursor;
        private int pos = 0, end = 0;
        private long[] ks, vs;

        EntryIterator() {
            this(keys, values);
        }

        EntryIterator(HugeLongArray keys, HugeLongArray values) {
            keyCursor = keys.initCursor(keys.newCursor());
            valueCursor = values.initCursor(values.newCursor());
            cursor = new LongLongCursor();
        }

        EntryIterator reset() {
            return reset(keys, values);
        }

        EntryIterator reset(HugeLongArray keys, HugeLongArray values) {
            keyCursor = keys.initCursor(keyCursor);
            valueCursor = values.initCursor(valueCursor);
            pos = 0;
            end = 0;
            hasNext = false;
            nextFetched = false;
            return this;
        }

        @Override
        public boolean hasNext() {
            if (!nextFetched) {
                nextFetched = true;
                return hasNext = fetchNext();
            }
            return hasNext;
        }

        @Override
        public LongLongCursor next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            nextFetched = false;
            return cursor;
        }

        private boolean fetchNext() {
            long key;
            do {
                while (pos < end) {
                    if ((key = ks[pos]) != 0L) {
                        cursor.index = pos;
                        cursor.key = key - 1L;
                        cursor.value = vs[pos];
                        ++pos;
                        return true;
                    }
                    ++pos;
                }
            } while (nextPage());
            return false;
        }

        private boolean nextPage() {
            return nextPage(keyCursor, valueCursor);
        }

        private boolean nextPage(
                final HugeCursor<long[]> keys,
                final HugeCursor<long[]> values) {
            boolean valuesHasNext = values.next();
            if (!keys.next()) {
                assert !valuesHasNext;
                return false;
            }
            assert valuesHasNext;
            ks = keys.array;
            pos = keys.offset;
            end = keys.limit;
            vs = values.array;
            assert pos == values.offset;
            assert end == values.limit;

            return true;
        }

        @Override
        public Iterator<LongLongCursor> iterator() {
            return reset();
        }

        @Override
        public void close() {
            keyCursor.close();
            keyCursor = null;
            valueCursor.close();
            valueCursor = null;
            cursor = null;
        }
    }
}
