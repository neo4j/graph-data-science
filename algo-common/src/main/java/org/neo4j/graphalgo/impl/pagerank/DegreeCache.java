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
package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

public class DegreeCache {

    public static final DegreeCache EMPTY = new DegreeCache(
            HugeDoubleArray.newArray(0, AllocationTracker.EMPTY),
            HugeObjectArray.newArray(HugeDoubleArray.class, 0, AllocationTracker.EMPTY),
            0.0);

    private final HugeDoubleArray aggregatedDegrees;
    private final HugeObjectArray<HugeDoubleArray> weights;
    private final double averageDegree;

    public DegreeCache(HugeDoubleArray aggregatedDegrees, HugeObjectArray<HugeDoubleArray> weights, double averageDegree) {
        this.aggregatedDegrees = aggregatedDegrees;
        this.weights = weights;
        this.averageDegree = averageDegree;
    }

    HugeDoubleArray aggregatedDegrees() {
        return aggregatedDegrees;
    }

    HugeObjectArray<HugeDoubleArray> weights() {
        return weights;
    }

    double average() {
        return averageDegree;
    }

    public DegreeCache withAverage(double newAverage) {
        return new DegreeCache(aggregatedDegrees, weights, newAverage);
    }
}
