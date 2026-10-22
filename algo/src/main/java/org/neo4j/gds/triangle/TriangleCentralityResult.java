/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.triangle;

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;

public record TriangleCentralityResult(
    HugeDoubleArray centralities,
    long globalTriangleCount,
    HugeAtomicLongArray triangleCounts,
    HugeAtomicLongArray triangleNeighbors,
    HugeAtomicLongArray allNeighbors,
    HugeAtomicLongArray triangleNeighborsTriangleSums,
    HugeAtomicLongArray allNeighborsTriangleSums
) {
    private static final HugeAtomicLongArray EMPTY_LONG_ARRAY =
        HugeAtomicLongArray.of(0, ParalleLongPageCreator.passThrough(new Concurrency(1)));

    public static final TriangleCentralityResult EMPTY = new TriangleCentralityResult(
        HugeDoubleArray.newArray(0),
        0,
        EMPTY_LONG_ARRAY,
        EMPTY_LONG_ARRAY,
        EMPTY_LONG_ARRAY,
        EMPTY_LONG_ARRAY,
        EMPTY_LONG_ARRAY
    );
}
