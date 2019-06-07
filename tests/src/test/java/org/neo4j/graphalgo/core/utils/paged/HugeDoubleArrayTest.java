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

import org.junit.Test;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import static org.junit.Assert.assertEquals;

public final class HugeDoubleArrayTest extends HugeArrayTestBase<double[], Double, HugeDoubleArray> {

    @Test
    public final void shouldAddToValues() {
        testArray(10, array -> {
            int index = between(2, 8);
            int value = between(42, 1337);
            array.set(index, value);
            int newValue = between(42, 1337);
            array.addTo(index, newValue);
            assertEquals(value + newValue, array.get(index), 1e-4);
        });
    }

    @Override
    HugeDoubleArray singleArray(final int size) {
        return HugeDoubleArray.newSingleArray(size, AllocationTracker.EMPTY);
    }

    @Override
    HugeDoubleArray pagedArray(final int size) {
        return HugeDoubleArray.newPagedArray(size, AllocationTracker.EMPTY);
    }

    @Override
    long bufferSize(final int size) {
        return MemoryUsage.sizeOfDoubleArray(size);
    }

    @Override
    Double box(final int value) {
        return (double) value;
    }

    @Override
    int unbox(final Double value) {
        return value.intValue();
    }
}
