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
package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.core.huge.HugeAdjacencyList;
import org.neo4j.graphalgo.core.huge.HugeAdjacencyOffsets;

class Relationships {
    private final long rows;
    private final long relationshipCount;
    private final HugeAdjacencyList inAdjacency;
    private final HugeAdjacencyList outAdjacency;
    private final HugeAdjacencyOffsets inOffsets;
    private final HugeAdjacencyOffsets outOffsets;
    private final double defaultWeight;
    private final HugeAdjacencyList inWeights;
    private final HugeAdjacencyList outWeights;
    private final HugeAdjacencyOffsets inWeightOffsets;
    private final HugeAdjacencyOffsets outWeightOffsets;

    Relationships(
            long rows,
            long relationshipCount,
            HugeAdjacencyList inAdjacency,
            HugeAdjacencyList outAdjacency,
            HugeAdjacencyOffsets inOffsets,
            HugeAdjacencyOffsets outOffsets,
            double defaultWeight,
            HugeAdjacencyList inWeights,
            HugeAdjacencyList outWeights,
            HugeAdjacencyOffsets inWeightOffsets,
            HugeAdjacencyOffsets outWeightOffsets) {
        this.rows = rows;
        this.relationshipCount = relationshipCount;
        this.inAdjacency = inAdjacency;
        this.outAdjacency = outAdjacency;
        this.inOffsets = inOffsets;
        this.outOffsets = outOffsets;
        this.defaultWeight = defaultWeight;
        this.inWeights = inWeights;
        this.outWeights = outWeights;
        this.inWeightOffsets = inWeightOffsets;
        this.outWeightOffsets = outWeightOffsets;
    }

    public long rows() {
        return rows;
    }

    long relationshipCount() { return relationshipCount; }

    HugeAdjacencyList inAdjacency() { return inAdjacency; }

    HugeAdjacencyList outAdjacency() { return outAdjacency; }

    HugeAdjacencyOffsets inOffsets() { return inOffsets; }

    HugeAdjacencyOffsets outOffsets() { return outOffsets; }

    double defaultWeight() { return defaultWeight; }

    HugeAdjacencyList inWeights() { return inWeights; }

    HugeAdjacencyList outWeights() { return outWeights; }

    HugeAdjacencyOffsets inWeightOffsets() { return inWeightOffsets; }

    HugeAdjacencyOffsets outWeightOffsets() { return outWeightOffsets; }
}
