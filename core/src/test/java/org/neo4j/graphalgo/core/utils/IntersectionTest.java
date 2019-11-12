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

import com.carrotsearch.hppc.LongHashSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IntersectionTest {

    private final long[][][] data = {
            {{1,2,4},{1,3,5},{1}},
            {{1,2,4},{1,2,5},{2}},
            {{1,2,4},{1,2,4},{3}},
            {{1,2,4},{1,2,4,5},{3}},
            {{1,2,4,5},{1,4,5},{3}},
            {{},{},{0}},
            {{},{1},{0}},
            {{1},{},{0}},
            {{1},{1},{1}},
            {{1},{0},{0}},
            {{0},{1},{0}},
            {{1,2,4,5},{1,5},{2}},
            {{1,2,4,5},{1,2},{2}},
            {{1,2,4,5},{1,4},{2}},
            {{1,2,4,5},{2,4},{2}},
            {{1,2,4,5},{2,5},{2}},
            {{1,2,4},{0,3,5},{0}}
    };

    @Test
    void intersection() {
        for (long[][] row : data) {
            assertEquals(row[2][0], Intersections.intersection(LongHashSet.from(row[0]),LongHashSet.from(row[1])));
        }
    }

    @Test
    void intersection2() {
        for (long[][] row : data) {
            assertEquals(row[2][0], Intersections.intersection2(row[0],row[1]));
        }
    }

    @Test
    void intersection3() {
        for (long[][] row : data) {
            assertEquals(row[2][0], Intersections.intersection3(row[0],row[1]), Arrays.toString(row));
        }
    }

    @Test
    void intersection4() {
        for (long[][] row : data) {
            assertEquals(row[2][0], Intersections.intersection4(row[0],row[1]), Arrays.toString(row));
        }
    }
}
