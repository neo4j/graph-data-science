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

public class HugeLongMatrix {

    private final HugeLongArray array;

    private final long rows;
    private final long cols;

    public HugeLongMatrix(long rows, long cols, AllocationTracker tracker) {
        this.rows = rows;
        this.cols = cols;
        this.array = HugeLongArrayBuilder.of(Math.multiplyExact(rows, cols), tracker).build();
    }

    public void set(long x, long y, long v) {
        assert x < rows;
        assert y < cols;
        array.set(indexOf(x, y), v);
    }

    public long get(long x, long y) {
        assert x < rows;
        assert y < cols;
        return array.get(indexOf(x, y));
    }

    private long indexOf(long x, long y) {
        return x * rows + y;
    }
}
