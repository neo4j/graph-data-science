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

import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public class RawValues {

    public static final IdCombiner OUTGOING = RawValues::combineIntInt;
    public static final IdCombiner INCOMING = (h, t) -> RawValues.combineIntInt(t, h);

    /**
     * shifts head into the most significant 4 bytes of the long
     * and places the tail in the least significant bytes
     *
     * @param head an arbitrary int value
     * @param tail an arbitrary int value
     * @return combination of head and tail
     */
    public static long combineIntInt(int head, int tail) {
        return ((long) head << 32) | (long) tail & 0xFFFFFFFFL;
    }

    public static long combineIntInt(Direction direction, int head, int tail) {
        if (direction == Direction.INCOMING) {
            return combineIntInt(tail, head);
        }
        return combineIntInt(head, tail);
    }

    public static IdCombiner combiner(Direction direction) {
        return (direction == Direction.INCOMING) ? INCOMING : OUTGOING;
    }

    /**
     * get the head value
     *
     * @param combinedValue a value built of 2 ints
     * @return the most significant 4 bytes as int
     */
    public static int getHead(long combinedValue) {
        return (int) (combinedValue >> 32);
    }

    /**
     * get the tail value
     *
     * @param combinedValue a value built of 2 ints
     * @return the least significant 4 bytes as int
     */
    public static int getTail(long combinedValue) {
        return (int) combinedValue;
    }
}
