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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public class NonWeightedComputeStep extends BaseComputeStep implements RelationshipConsumer {

    private float srcRankDelta;

    NonWeightedComputeStep(
        double dampingFactor,
        double toleranceValue,
        long[] sourceNodeIds,
        Graph graph,
        AllocationTracker tracker,
        int partitionSize,
        long startNode,
        ProgressLogger progressLogger
    ) {
        super(
            dampingFactor,
            toleranceValue,
            sourceNodeIds,
            graph,
            tracker,
            partitionSize,
            startNode,
            progressLogger
        );
    }

    @Override
    void singleIteration() {
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
            double delta = deltas[(int) (nodeId - startNode)];
            // avoids rank computation
            // TODO: is this equivalent of not receiving messages in Pregel?
            if (delta > 0.0) {
                int degree = degrees.degree(nodeId);
                if (degree > 0) {
                    // this will be the value that we "send" to our neighbors
                    srcRankDelta = (float) (delta / degree);
                    relationshipIterator.forEachRelationship(nodeId, this);
                }
            }
            progressLogger.logProgress(graph.degree(nodeId));
        }
    }

    @Override
    public boolean accept(long sourceNodeId, long targetNodeId) {
        if (srcRankDelta != 0F) {
            // idx is partition id where the target lives
            int idx = binaryLookup(targetNodeId, starts);
            int relativeTargetNodeId = (int) (targetNodeId - starts[idx]);
            nextScores[idx][relativeTargetNodeId] += srcRankDelta;
        }
        return true;
    }
}
