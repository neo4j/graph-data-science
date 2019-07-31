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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class EigenvectorCentralityComputeStep extends BaseComputeStep implements RelationshipConsumer {
    private float srcRankDelta;
    private final double initialValue;

    EigenvectorCentralityComputeStep(
            double dampingFactor,
            long[] sourceNodeIds,
            Graph graph,
            AllocationTracker tracker,
            int partitionSize,
            long startNode,
            long nodeCount) {
        super(dampingFactor,
                sourceNodeIds,
                graph,
                tracker,
                partitionSize,
                startNode);
        this.initialValue = 1.0 / nodeCount;
    }

    @Override
    protected double initialValue() {
        return initialValue;
    }

    void singleIteration() {
        long startNode = this.startNode;
        long endNode = this.endNode;
        RelationshipIterator rels = this.relationshipIterator;
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
            double delta = deltas[(int) (nodeId - startNode)];
            if (delta > 0.0) {
                int degree = degrees.degree(nodeId, direction);
                if (degree > 0) {
                    srcRankDelta = (float) delta;
                    rels.forEachRelationship(nodeId, direction, this);
                }
            }
        }
    }

    @Override
    public boolean accept(long sourceNodeId, long targetNodeId) {
        if (srcRankDelta != 0F) {
            int idx = binaryLookup(targetNodeId, starts);
            nextScores[idx][(int) (targetNodeId - starts[idx])] += srcRankDelta;
        }
        return true;
    }

    @Override
    boolean combineScores() {
        assert prevScores != null;
        assert prevScores.length >= 1;

        double[] pageRank = this.pageRank;
        double[] deltas = this.deltas;
        float[][] prevScores = this.prevScores;
        int length = prevScores[0].length;

        boolean shouldBreak = true;

        for (int i = 0; i < length; i++) {
            double delta = 0.0;
            for (float[] scores : prevScores) {
                delta += (double) scores[i];
                scores[i] = 0F;
            }
            if (delta > tolerance) {
                shouldBreak = false;
            }
            pageRank[i] += delta;
            deltas[i] = delta;
        }

        return shouldBreak;
    }

    @Override
    void normalizeDeltas() {
        for (int i = 0; i < deltas.length; i++) {
            deltas[i] = deltas[i] / l2Norm;
        }
    }

}
