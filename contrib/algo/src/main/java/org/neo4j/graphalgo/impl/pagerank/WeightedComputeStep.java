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

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.api.WeightedRelationshipIterator;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class WeightedComputeStep extends BaseComputeStep implements WeightedRelationshipConsumer {
    private final double[] aggregatedDegrees;
    private final WeightedRelationshipIterator relationshipIterator;
    private double sumOfWeights;
    private double delta;

    WeightedComputeStep(
            double dampingFactor,
            int[] sourceNodeIds,
            WeightedRelationshipIterator weightedRelationshipIterator,
            Degrees degrees,
            int partitionSize,
            int startNode,
            DegreeCache degreeCache) {
        super(dampingFactor,
                sourceNodeIds,
                degrees,
                partitionSize,
                startNode);
        this.relationshipIterator = weightedRelationshipIterator;
        this.aggregatedDegrees = degreeCache.aggregatedDegrees();
    }

    void singleIteration() {
        int startNode = this.startNode;
        int endNode = this.endNode;
        WeightedRelationshipIterator rels = this.relationshipIterator;
        for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
            delta = deltas[nodeId - startNode];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    sumOfWeights = aggregatedDegrees[nodeId];

                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                }
            }
        }
    }

    @Override
    public boolean accept(int sourceNodeId, int targetNodeId, long relationId, double weight) {
        if (weight > 0) {
            double proportion = weight / sumOfWeights;
            float srcRankDelta = (float) (delta * proportion);
            if (srcRankDelta != 0f) {
                int idx = binaryLookup(targetNodeId, starts);
                nextScores[idx][targetNodeId - starts[idx]] += srcRankDelta;
            }
        }

        return true;
    }
}
