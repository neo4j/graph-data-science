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
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class CommonNeighbourAwareNextNodeStrategy implements NextNodeStrategy {

    final private Graph inputGraph;
    final private boolean isWeighted;
    final private SplittableRandom rng;

    public CommonNeighbourAwareNextNodeStrategy(
        Graph inputGraph,
        boolean isWeighted,
        SplittableRandom rng
    ) {
        this.inputGraph = inputGraph;
        this.isWeighted = isWeighted;
        this.rng = rng;
    }

    @Override
    public long getNextNode(long currentNode) {
        double q, chanceOutOfNeighbours;
        long candidateNode;
        var uSortedNeighs = new SortedNeighsWithWeights(inputGraph, currentNode, isWeighted);
        do {
            candidateNode = getCandidateNode(uSortedNeighs);
            var vSortedNeighs = new SortedNeighsWithWeights(inputGraph, candidateNode, isWeighted);
            var overlap = computeOverlap(uSortedNeighs, vSortedNeighs);

            chanceOutOfNeighbours = 1.0D - overlap;
            q = rng.nextDouble();
        } while (q > chanceOutOfNeighbours);

        return candidateNode;
    }

    private long getCandidateNode(SortedNeighsWithWeights neighsWithWeights) {
        long candidateNode;
        if (isWeighted) {
            assert neighsWithWeights.getWeights().isPresent();
            candidateNode = weightedNextNode(neighsWithWeights.getNeighs(), neighsWithWeights.getWeights().get());
        } else {
            int targetOffsetCandidate = rng.nextInt(neighsWithWeights.getNeighs().length);
            candidateNode = neighsWithWeights.getNeighs()[targetOffsetCandidate];
            assert candidateNode != IdMap.NOT_FOUND : "The offset '" + targetOffsetCandidate +
                                                      "' is bound by the degree but no target could be found for nodeId " + candidateNode;
        }
        return candidateNode;
    }

    private double computeOverlap(SortedNeighsWithWeights uNeighs, SortedNeighsWithWeights vNeighs) {
        return computeOverlapSimilarity(
            uNeighs.getNeighs(),
            uNeighs.getWeights(),
            vNeighs.getNeighs(),
            vNeighs.getWeights()
        );
    }

    private double computeOverlapSimilarity(
        long[] neighsU, Optional<double[]> weightsU, long[] neighsV, Optional<double[]> weightsV
    ) {
        double similarity;
        if (isWeighted) {
            assert weightsU.isPresent();
            assert weightsV.isPresent();
            similarity = OverlapSimilarity.computeWeightedSimilarity(neighsU, neighsV, weightsU.get(), weightsV.get());
        } else {
            similarity = OverlapSimilarity.computeSimilarity(neighsU, neighsV);
        }
        if (!Double.isNaN(similarity))
            return similarity;
        else
            return 0.0D;
    }

    private long weightedNextNode(long[] neighs, double[] weights) {
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
        Optional<double[]> weights;

        SortedNeighsWithWeights(Graph graph, long nodeId, boolean isWeighted) {
            this.neighs = new long[graph.degree(nodeId)];
            this.weights = Optional.empty();
            var idx = new AtomicInteger(0);
            if (!isWeighted) {
                graph.forEachRelationship(nodeId, (src, dst) -> {
                    neighs[idx.getAndIncrement()] = dst;
                    return true;
                });
            } else {
                double[] weightsArray = new double[graph.degree(nodeId)];
                graph.forEachRelationship(nodeId, 0.0, (src, dst, w) -> {
                    var localIdx = idx.getAndIncrement();
                    neighs[localIdx] = dst;
                    weightsArray[localIdx] = w;
                    return true;
                });

                weights = Optional.of(IntStream.range(0, weightsArray.length).boxed()
                    .sorted((i, j) -> Long.compare(neighs[i], neighs[j]))
                    .map(i -> weightsArray[i]).mapToDouble(x -> x).toArray());
            }
            Arrays.sort(neighs);
        }

        long[] getNeighs() {
            return neighs;
        }

        Optional<double[]> getWeights() {
            return weights;
        }
    }
}
