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
package org.neo4j.graphalgo.core.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author mknblch
 */
class AtomicDoubleArrayTest {

    private final AtomicDoubleArray array =
            new AtomicDoubleArray(1);

    @Test
    void testSetValue() {
        array.set(0, 1234.5678);
        assertEquals(1234.5678, array.get(0), 0.01);
    }

    @Test
    void testSetInfinity() {
        array.set(0, Double.POSITIVE_INFINITY);
        assertEquals(Double.POSITIVE_INFINITY, array.get(0), 0.01);
        array.set(0, Double.NEGATIVE_INFINITY);
        assertEquals(Double.NEGATIVE_INFINITY, array.get(0), 0.01);
    }

    @Test
    void testAddValue() {
        array.set(0, 123.4);
        array.add(0, 123.4);
        assertEquals(246.8, array.get(0), 0.1);
    }

    @Test
    void testAddInf() {
        array.set(0, Double.POSITIVE_INFINITY);
        array.add(0, 123.4);
        assertEquals(Double.POSITIVE_INFINITY, array.get(0), 0.1);
    }

    @Test
    void testAddInfMax() {
        array.set(0, Double.MAX_VALUE);
        array.add(0, 123.4);
        assertEquals(Double.MAX_VALUE, array.get(0), 0.1);
    }
}