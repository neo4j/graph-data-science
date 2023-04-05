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
package org.neo4j.gds.graphsampling.samplers.rw.cnarw;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.functions.similairty.OverlapSimilarity;
import org.neo4j.gds.graphsampling.samplers.rw.NextNodeStrategy;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class CommonNeighbourAwareNextNodeStrategy implements NextNodeStrategy {

    private final Graph inputGraph;
    private final SplittableRandom rng;

    CommonNeighbourAwareNextNodeStrategy(
        Graph inputGraph,
        SplittableRandom rng
    ) {
        this.inputGraph = inputGraph;
        this.rng = rng;
    }

    @Override
    public long getNextNode(long currentNode) {
        double q, chanceOutOfNeighbours;
        long candidateNode;
        var uSortedNeighs = sortedNeighbours(inputGraph, currentNode);
        do {
            candidateNode = getCandidateNode(uSortedNeighs);
            var vSortedNeighs = sortedNeighbours(inputGraph, candidateNode);
            var overlap = computeOverlapSimilarity(uSortedNeighs, vSortedNeighs);

            chanceOutOfNeighbours = 1.0D - overlap;
            q = rng.nextDouble();
        } while (q > chanceOutOfNeighbours);

        return candidateNode;
    }

    private double computeOverlapSimilarity(long[] neighsU, long[] neighsV) {
        double similarity = OverlapSimilarity.computeSimilarity(neighsU, neighsV);
        if (Double.isNaN(similarity)) {
            return 0.0D;
        }
        return similarity;
    }

    private long getCandidateNode(long[] sortedNeighs) {
        long candidateNode;

        int targetOffsetCandidate = rng.nextInt(sortedNeighs.length);
        candidateNode = sortedNeighs[targetOffsetCandidate];
        assert candidateNode != IdMap.NOT_FOUND : "The offset '" + targetOffsetCandidate +
                                                  "' is bound by the degree but no target could be found for nodeId " + candidateNode;
        return candidateNode;
    }



    private long[] sortedNeighbours(Graph graph, long nodeId) {
        var neighs = new long[graph.degree(nodeId)];
        var idx = new AtomicInteger(0);
        graph.forEachRelationship(nodeId, (src, dst) -> {
            neighs[idx.getAndIncrement()] = dst;
            return true;
        });
        Arrays.sort(neighs);
        return neighs;
    }
}
