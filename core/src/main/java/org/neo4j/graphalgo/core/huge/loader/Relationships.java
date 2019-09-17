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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;

class Relationships {
    private final long rows;
    private final long relationshipCount;
    private final AdjacencyList inAdjacency;
    private final AdjacencyList outAdjacency;
    private final AdjacencyOffsets inOffsets;
    private final AdjacencyOffsets outOffsets;
    private final double defaultWeight;
    private final AdjacencyList inWeights;
    private final AdjacencyList outWeights;
    private final AdjacencyOffsets inWeightOffsets;
    private final AdjacencyOffsets outWeightOffsets;

    Relationships(
            long rows,
            long relationshipCount,
            AdjacencyList inAdjacency,
            AdjacencyList outAdjacency,
            AdjacencyOffsets inOffsets,
            AdjacencyOffsets outOffsets,
            double defaultWeight,
            AdjacencyList inWeights,
            AdjacencyList outWeights,
            AdjacencyOffsets inWeightOffsets,
            AdjacencyOffsets outWeightOffsets) {
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

    AdjacencyList inAdjacency() { return inAdjacency; }

    AdjacencyList outAdjacency() { return outAdjacency; }

    AdjacencyOffsets inOffsets() { return inOffsets; }

    AdjacencyOffsets outOffsets() { return outOffsets; }

    double defaultWeight() { return defaultWeight; }

    AdjacencyList inWeights() { return inWeights; }

    AdjacencyList outWeights() { return outWeights; }

    AdjacencyOffsets inWeightOffsets() { return inWeightOffsets; }

    AdjacencyOffsets outWeightOffsets() { return outWeightOffsets; }
}
