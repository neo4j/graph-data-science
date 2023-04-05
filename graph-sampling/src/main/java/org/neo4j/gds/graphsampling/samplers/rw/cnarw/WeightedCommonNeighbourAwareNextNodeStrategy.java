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
import org.neo4j.gds.functions.similairty.OverlapSimilarity;
import org.neo4j.gds.graphsampling.samplers.rw.NextNodeStrategy;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class WeightedCommonNeighbourAwareNextNodeStrategy implements NextNodeStrategy {

    private final Graph inputGraph;
    private final SplittableRandom rng;

    WeightedCommonNeighbourAwareNextNodeStrategy(
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
        var uSortedNeighs = new SortedNeighsWithWeights(inputGraph, currentNode);
        do {
            candidateNode = getCandidateNode(uSortedNeighs.getNeighs(), uSortedNeighs.getWeights());
            var vSortedNeighs = new SortedNeighsWithWeights(inputGraph, candidateNode);
            var overlap = computeOverlapSimilarity(
                uSortedNeighs.getNeighs(),
                uSortedNeighs.getWeights(),
                vSortedNeighs.getNeighs(),
                vSortedNeighs.getWeights()
            );

            chanceOutOfNeighbours = 1.0D - overlap;
            q = rng.nextDouble();
        } while (q > chanceOutOfNeighbours);

        return candidateNode;
    }

    private double computeOverlapSimilarity(long[] neighsU, double[] weightsU, long[] neighsV, double[] weightsV) {
        double similarity = OverlapSimilarity.computeWeightedSimilarity(neighsU, neighsV, weightsU, weightsV);
        if (Double.isNaN(similarity)) {
            return 0.0D;
        }
        return similarity;
    }

    private long getCandidateNode(long[] neighs, double[] weights) {
        var sumWeights = Arrays.stream(weights).sum();
        var remainingMass = rng.nextDouble(0, sumWeights);

        int i = 0;
        while (remainingMass > 0) {
            remainingMass -= weights[i++];
        }

        return neighs[i - 1];
    }


    static class SortedNeighsWithWeights {
        final long[] neighs;
        final double[] weights;

        SortedNeighsWithWeights(Graph graph, long nodeId) {
            this.neighs = new long[graph.degree(nodeId)];
            var idx = new AtomicInteger(0);
            double[] weightsArray = new double[graph.degree(nodeId)];
            graph.forEachRelationship(nodeId, 0.0, (src, dst, w) -> {
                var localIdx = idx.getAndIncrement();
                neighs[localIdx] = dst;
                weightsArray[localIdx] = w;
                return true;
            });

            weights = IntStream.range(0, weightsArray.length).boxed()
                .sorted((i, j) -> Long.compare(neighs[i], neighs[j]))
                .map(i -> weightsArray[i]).mapToDouble(x -> x).toArray();
            Arrays.sort(neighs);
        }

        long[] getNeighs() {
            return neighs;
        }

        double[] getWeights() {
            return weights;
        }
    }
}
