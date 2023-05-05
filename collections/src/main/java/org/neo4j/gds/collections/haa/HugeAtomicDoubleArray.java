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
package org.neo4j.gds.collections.haa;

import org.neo4j.gds.collections.HugeAtomicArray;
import org.neo4j.gds.collections.cursor.HugeCursorSupport;

import static org.neo4j.gds.collections.haa.ValueTransformers.DoubleToDoubleFunction;

@HugeAtomicArray(valueType = double.class, valueOperatorInterface = DoubleToDoubleFunction.class, pageCreatorInterface = PageCreator.DoublePageCreator.class)
public abstract class HugeAtomicDoubleArray implements HugeCursorSupport<double[]> {

    /**
     * Creates a new array of the given size.
     *
     * @param size the length of the new array, the highest supported index is {@code size - 1}
     * @return new array
     */
    public static HugeAtomicDoubleArray of(long size, PageCreator.DoublePageCreator pageCreator) {
        return HugeAtomicDoubleArrayFactory.of(size, pageCreator);
    }

    public static long memoryEstimation(long size) {
        return HugeAtomicDoubleArrayFactory.memoryEstimation(size);
    }

    /**
     *
     * @return the defaultValue to fill the remaining space in the input of {@link #copyTo(org.neo4j.gds.collections.haa.HugeAtomicDoubleArray, long)}.
     */
    public double defaultValue() {
        return 0.0;
    }

    /**
     * @return the long value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract double get(long index);

    /**
     * Atomically adds the given delta to the value at the given index.
     *
     * @param index the index
     * @param delta the value to add
     * @return the previous value at index
     */
    public abstract double getAndAdd(long index, double delta);

    /**
     * Atomically returns the value at the given index and replaces it with the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract double getAndReplace(long index, double value);


    /**
     * Sets the long value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void set(long index, double value);

    /**
     * Atomically sets the element at position {@code index} to the given
     * updated value if the current value {@code ==} the expected value.
     *
     * @param index  the index
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} iff successful. {@code false} indicates that the actual
     *     value was not equal to the expected value.
     */
    public abstract boolean compareAndSet(long index, double expect, double update);

    /**
     * Atomically sets the element at position {@code index} to the given
     * updated value if the current value, referred to as the <em>witness value</em>,
     * {@code ==} the expected value.
     *
     * This operation works as if implemented as
     *
     * <pre>
     *     if (this.compareAndSet(index, expect, update)) {
     *         return expect;
     *     } else {
     *         return this.get(index);
     *     }
     * </pre>
     *
     * The actual implementation is done with a single atomic operation so that the
     * returned witness value is the value that was failing the update, not one that
     * needs be read again after the failed update.
     *
     * This allows one to write CAS-loops in a different way, which removes
     * one volatile read per loop iteration
     *
     * <pre>
     *     var oldValue = this.get(index);
     *     while (true) {
     *         var newValue = updateFunction(oldValue);
     *         var witnessValue = this.compareAndExchange(index, oldValue, newValue);
     *         if (witnessValue == oldValue) {
     *             // update successful
     *             break;
     *         }
     *         // update unsuccessful set, loop and try again.
     *         // Here we already have the updated witness value and don't need to issue
     *         // a new read
     *         oldValue = witnessValue;
     *     }
     * </pre>
     *
     * @param index  the index
     * @param expect the expected value
     * @param update the new value
     * @return the result that is the witness value,
     *     which will be the same as the expected value if successful≤
     *     or the new current value if unsuccessful.
     */
    public abstract double compareAndExchange(long index, double expect, double update);

    /**
     * Atomically updates the element at index {@code index} with the results
     * of applying the given function, returning the updated value. The
     * function should be side-effect-free, since it may be re-applied
     * when attempted updates fail due to contention among threads.
     *
     * @param index          the index
     * @param updateFunction a side-effect-free function
     */
    public abstract void update(long index, DoubleToDoubleFunction updateFunction);

    /**
     * Returns the length of this array.
     * <p>
     * If the size is greater than zero, the highest supported index is {@code size() - 1}
     * <p>
     * The behavior is identical to calling {@code array.length} on primitive arrays.
     */
    public abstract long size();

    /**
     * @return the amount of memory used by the instance of this array, in bytes.
     *     This should be the same as returned from {@link #release()} without actually releasing the array.
     */
    public abstract long sizeOf();

    /**
     * Sets all entries in the array to the given value.
     *
     * This method is not thread-safe.
     */
    public abstract void setAll(double value);

    /**
     * Destroys the data, allowing the underlying storage arrays to be collected as garbage.
     * The array is unusable after calling this method and will throw {@link NullPointerException}s
     * on virtually every method invocation.
     * <p>
     * Note that the data might not immediately collectible if there are still cursors alive that
     * reference this array. You have to {@link org.neo4j.gds.collections.cursor.HugeCursor#close()} every cursor instance as well.
     * <p>
     * The amount is not removed from the {@link java.util.function.LongConsumer} that had been
     * provided in the constructor.
     *
     * @return the amount of memory freed, in bytes.
     */
    public abstract long release();

    /**
     * Copies the content of this array into the target array.
     * <p>
     * The behavior is identical to {@link System#arraycopy(Object, int, Object, int, int)}.
     * <p>
     * This method is not thread-safe.
     */
    public abstract void copyTo(HugeAtomicDoubleArray dest, long length);
}
