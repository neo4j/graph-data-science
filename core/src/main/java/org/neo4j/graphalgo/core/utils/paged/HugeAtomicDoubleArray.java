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

import java.util.function.DoubleUnaryOperator;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongUnaryOperator;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;

public class HugeAtomicDoubleArray {

    private final HugeAtomicLongArray data;

    public HugeAtomicDoubleArray(HugeAtomicLongArray data) {
        this.data = data;
    }

    public double get(long index) {
        return Double.longBitsToDouble(data.get(index));
    }

    /**
     * Sets the long value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public void set(long index, double value) {
        data.set(index, Double.doubleToLongBits(value));
    }

    /**
     * Atomically updates the element at index {@code index} with the results
     * of applying the given function, returning the updated value. The
     * function should be side-effect-free, since it may be re-applied
     * when attempted updates fail due to contention among threads.
     *
     * @param index          the index
     * @param updateFunction a side-effect-free function
     */
    public void update(long index, DoubleUnaryOperator updateFunction) {
        LongUnaryOperator longUpdateFunction = (oldLongValue) -> {
            double oldDoubleValue = Double.longBitsToDouble(oldLongValue);
            double newDoubleValue = updateFunction.applyAsDouble(oldDoubleValue);
            return Double.doubleToLongBits(newDoubleValue);
        };

        data.update(index, longUpdateFunction);
    }

    /**
     * Returns the length of this array.
     * <p>
     * If the size is greater than zero, the highest supported index is {@code size() - 1}
     * <p>
     * The behavior is identical to calling {@code array.length} on primitive arrays.
     */
    public long size() {
        return data.size();
    }

    /**
     * @return the amount of memory used by the instance of this array, in bytes.
     *     This should be the same as returned from {@link #release()} without actually releasing the array.
     */
    public long sizeOf() {
        return data.sizeOf();
    }

    public boolean compareAndSet(long index, double expect, double update) {
        return data.compareAndSet(index, Double.doubleToLongBits(expect), Double.doubleToLongBits(update));
    }

    /**
     * Destroys the data, allowing the underlying storage arrays to be collected as garbage.
     * The array is unusable after calling this method and will throw {@link NullPointerException}s on virtually every method invocation.
     * <p>
     * Note that the data might not immediately collectible if there are still cursors alive that reference this array.
     * You have to {@link HugeCursor#close()} every cursor instance as well.
     * <p>
     * The amount is not removed from the {@link AllocationTracker} that had been provided in the constructor.
     *
     * @return the amount of memory freed, in bytes.
     */
    public long release() {
        return data.release();
    }

    /**
     * Creates a new array of the given size, tracking the memory requirements into the given {@link AllocationTracker}.
     * The tracker is no longer referenced, as the arrays do not dynamically change their size.
     */
    public static HugeAtomicDoubleArray newArray(long size, AllocationTracker tracker) {
        return new HugeAtomicDoubleArray(HugeAtomicLongArray.newArray(size, null, tracker));
    }

    public static HugeAtomicDoubleArray newArray(long size, LongToDoubleFunction gen, AllocationTracker tracker) {
        return new HugeAtomicDoubleArray(HugeAtomicLongArray.newArray(size, convertLongDoubleFunctionToLongUnary(gen), tracker));
    }

    /* test-only */
    static HugeAtomicDoubleArray newPagedArray(long size, final LongToDoubleFunction gen, AllocationTracker tracker) {
        return new HugeAtomicDoubleArray(HugeAtomicLongArray.newPagedArray(size, convertLongDoubleFunctionToLongUnary(gen), tracker));
    }

    /* test-only */
    static HugeAtomicDoubleArray newSingleArray(int size, final LongToDoubleFunction gen, AllocationTracker tracker) {
        return new HugeAtomicDoubleArray(HugeAtomicLongArray.newSingleArray(size, convertLongDoubleFunctionToLongUnary(gen), tracker));
    }

    private static LongUnaryOperator convertLongDoubleFunctionToLongUnary(LongToDoubleFunction gen) {
        if (gen == null) {
            return null;
        }
        return (index) -> Double.doubleToLongBits(gen.applyAsDouble(index));
    }

    public static long memoryEstimation(long size) {
        assert size >= 0;

        long hugeLongArraySize = HugeAtomicLongArray.memoryEstimation(size);
        long instanceSize = sizeOfInstance(HugeAtomicDoubleArray.class);

        return instanceSize + hugeLongArraySize;
    }

}