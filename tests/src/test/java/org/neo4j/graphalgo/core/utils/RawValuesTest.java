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
package org.neo4j.graphalgo.core.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 *         added 30.03.2017.
 */
public class RawValuesTest {

    @Test
    public void testPositive() throws Exception {
        long combinedValue = RawValues.combineIntInt(42, 1337);
        assertEquals(42, RawValues.getHead(combinedValue));
        assertEquals(1337, RawValues.getTail(combinedValue));
    }

    @Test
    public void testNegative() throws Exception {
        long combinedValue = RawValues.combineIntInt(-1337, -42);
        assertEquals(-1337, RawValues.getHead(combinedValue));
        assertEquals(-42, RawValues.getTail(combinedValue));
    }

    @Test
    public void testMixed() throws Exception {
        long combinedValue = RawValues.combineIntInt(-42, 1337);
        assertEquals(-42, RawValues.getHead(combinedValue));
        assertEquals(1337, RawValues.getTail(combinedValue));

        combinedValue = RawValues.combineIntInt(42, -1337);
        assertEquals(42, RawValues.getHead(combinedValue));
        assertEquals(-1337, RawValues.getTail(combinedValue));

    }
}
