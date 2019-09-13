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

import com.carrotsearch.hppc.sorting.IndirectComparator;

public class AscendingLongComparator implements IndirectComparator {
    private final long[] array;

    public AscendingLongComparator(long[] array) {
        this.array = array;
    }

    public int compare(int indexA, int indexB) {
        long a = this.array[indexA];
        long b = this.array[indexB];
        if (a < b) {
            return -1;
        } else {
            return a > b ? 1 : 0;
        }
    }
}
