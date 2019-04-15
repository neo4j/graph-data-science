/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
public class AtomicDoubleArrayTest {

    final AtomicDoubleArray array =
            new AtomicDoubleArray(1);

    @Test
    public void testSetValue() throws Exception {
        array.set(0, 1234.5678);
        assertEquals(1234.5678, array.get(0), 0.01);
    }

    @Test
    public void testSetInfinity() throws Exception {
        array.set(0, Double.POSITIVE_INFINITY);
        assertEquals(Double.POSITIVE_INFINITY, array.get(0), 0.01);
        array.set(0, Double.NEGATIVE_INFINITY);
        assertEquals(Double.NEGATIVE_INFINITY, array.get(0), 0.01);
    }

    @Test
    public void testAddValue() throws Exception {
        array.set(0, 123.4);
        array.add(0, 123.4);
        assertEquals(246.8, array.get(0), 0.1);
    }

    @Test
    public void testAddInf() throws Exception {
        array.set(0, Double.POSITIVE_INFINITY);
        array.add(0, 123.4);
        assertEquals(Double.POSITIVE_INFINITY, array.get(0), 0.1);
    }

    @Test
    public void testAddInfMax() throws Exception {
        array.set(0, Double.MAX_VALUE);
        array.add(0, 123.4);
        assertEquals(Double.MAX_VALUE, array.get(0), 0.1);
    }

}