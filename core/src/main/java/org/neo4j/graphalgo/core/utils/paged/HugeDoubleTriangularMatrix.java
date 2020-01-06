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

import static org.neo4j.graphalgo.core.utils.paged.HugeMatrices.triangularIndex;

public class HugeDoubleTriangularMatrix {

    private final HugeDoubleArray array;
    private final long order;

    public HugeDoubleTriangularMatrix(long order, AllocationTracker tracker) {
        long size = Math.multiplyExact(order, order + 1) / 2;
        this.order = order;
        this.array = HugeDoubleArray.newArray(size, tracker);
    }

    public void set(long x, long y, double v) {
        array.set(indexOf(x, y), v);
    }

    public double get(long x, long y) {
        return array.get(indexOf(x, y));
    }

    public long order() {
        return order;
    }

    private long indexOf(long x, long y) {
        return triangularIndex(order, x, y);
    }

}
