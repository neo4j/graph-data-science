/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.compat;

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

/**
 * Please, never use the following methods with a unqualified static import, always call them qualified
 * with {@code UnsafeProxy.<…>()}, so that we can grep for {@code UnsafeProxy} and find all use-sites
 * of the Unsafe.
 * <p>
 * Always check for the availability of Unsafe using {@link #assertHasUnsafe()}.
 */
public final class UnsafeProxy {

    /**
     * @throws java.lang.LinkageError if the Unsafe tools are not available on in this JVM.
     */
    public static void assertHasUnsafe() {
        UnsafeUtil.assertHasUnsafe();
    }

    /**
     * Report the offset of the first element in the storage allocation of a
     * given array class.  If {@link #arrayIndexScale} returns a non-zero value
     * for the same class, you may use that scale factor, together with this
     * base offset, to form new offsets to access elements of arrays of the
     * given class.
     *
     * @see #getLongVolatile(long[], long)
     * @see #putLongVolatile(long[], long, long)
     */
    public static int arrayBaseOffset(Class<?> klass) {
        return UnsafeUtil.arrayBaseOffset(klass);
    }

    /**
     * Report the scale factor for addressing elements in the storage
     * allocation of a given array class.  However, arrays of "narrow" types
     * will generally not work properly with accessors,
     * so the scale factor for such classes is reported as zero.
     *
     * @see #arrayBaseOffset
     * @see #getLongVolatile(long[], long)
     * @see #putLongVolatile(long[], long, long)
     */
    public static int arrayIndexScale(Class<?> klass) {
        return UnsafeUtil.arrayIndexScale(klass);
    }

    /**
     * Fetches an array element within the given array <code>array</code>
     * at the given offset using the memory semantics of a volatile read.
     * <p>
     * The results are undefined unless one of the following case is true:
     * <p>
     * The object referred to by <code>array</code> is an array, and the offset
     * is an integer of the form <code>B+N*S</code>, where <code>N</code> is
     * a valid index into the array, and <code>B</code> and <code>S</code> are
     * the values obtained by {@link #arrayBaseOffset} and {@link
     * #arrayIndexScale} (respectively) from the array's class.  The value
     * referred to is the <code>N</code><em>th</em> element of the array.
     * <p>
     * If the above cases is true, the call references a specific Java
     * variable (array element).  However, the results are undefined
     * if that variable is not in fact of the type returned by this method.
     *
     * @param array  Java array in which the variable resides
     * @param offset indication of where the variable resides in the array
     * @return the value fetched from the indicated Java variable
     */
    public static long getLongVolatile(long[] array, long offset) {
        return UnsafeUtil.getLongVolatile(array, offset);
    }

    /**
     * Stores a value into a given Java variable using the memory semantics of a volatile write.
     * <p>
     * The first two parameters are interpreted exactly as with
     * {@link #getLongVolatile(long[], long)} to refer to a specific
     * Java variable (array element).  The given value
     * is stored into that variable.
     * <p>
     * The variable must be of the same type as the method
     * parameter <code>value</code>.
     *
     * @param array  Java array in which the variable resides
     * @param offset indication of where the variable resides in the array
     * @param value  the value to store into the indicated Java variable
     */
    public static void putLongVolatile(long[] array, long offset, long value) {
        UnsafeUtil.putLongVolatile(array, offset, value);
    }

    /**
     * Atomically compare the current value of the given long field with the expected value, and if they are the
     * equal, set the field to the updated value and return true. Otherwise return false.
     * <p>
     * If this method returns true, then it has the memory visibility semantics of a volatile read followed by a
     * volatile write.
     */
    public static boolean compareAndSwapLong(long[] array, long offset, long expect, long update) {
        return UnsafeUtil.compareAndSwapLong(array, offset, expect, update);
    }

    private UnsafeProxy() {
        throw new UnsupportedOperationException("No instances");
    }
}
