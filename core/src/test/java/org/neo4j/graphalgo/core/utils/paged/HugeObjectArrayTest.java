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
package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

final class HugeObjectArrayTest extends HugeArrayTestBase<String[], String, HugeObjectArray<String>> {

    @Override
    HugeObjectArray<String> singleArray(final int size) {
        return HugeObjectArray.newSingleArray(String.class, size, AllocationTracker.EMPTY);
    }

    @Override
    HugeObjectArray<String> pagedArray(final int size) {
        return HugeObjectArray.newPagedArray(String.class, size, AllocationTracker.EMPTY);
    }

    @Override
    long bufferSize(final int size) {
        return MemoryUsage.sizeOfObjectArray(size);
    }

    @Override
    String box(final int value) {
        return value + "";
    }

    @Override
    int unbox(final String value) {
        return value == null ? 0 : Integer.parseInt(value);
    }

    @Override
    String primitiveNull() {
        return null;
    }
}
