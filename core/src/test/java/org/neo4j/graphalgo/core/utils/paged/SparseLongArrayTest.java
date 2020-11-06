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


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.core.utils.paged.SparseLongArray.NOT_FOUND;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class SparseLongArrayTest {

    @Test
    void testEmpty() {
        var array = new SparseLongArray(42);
        assertEquals(NOT_FOUND, array.toMappedNodeId(23));
    }

    @Test
    void testZeroEntry() {
        var array = new SparseLongArray(42);
        array.set(0);
        assertEquals(0, array.toMappedNodeId(0));
    }

    @Test
    void testSingleEntry() {
        var array = new SparseLongArray(42);
        array.set(23);
        assertEquals(0, array.toMappedNodeId(23));
    }

    @Test
    void testMultipleEntries() {
        var capacity = 128;
        var array = new SparseLongArray(capacity);
        for (int i = 0; i < capacity; i+=2) {
            array.set(i);
        }
        for (int i = 0; i < capacity; i+=2) {
            assertEquals(i / 2, array.toMappedNodeId(i), formatWithLocale("wrong mapping for original id %d", i));
        }
    }

    @Test
    void testNonExisting() {
        var array = new SparseLongArray(42);
        array.set(23);
        assertEquals(NOT_FOUND, array.toMappedNodeId(24));
    }

}