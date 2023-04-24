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
import org.neo4j.gds.api.compress.LongArrayBuffer;
import org.neo4j.gds.functions.similairty.OverlapSimilarity;
import org.neo4j.gds.graphsampling.samplers.rw.NextNodeStrategy;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class WeightedCommonNeighbourAwareNextNodeStrategy implements NextNodeStrategy {

    private Graph inputGraph;
    private SplittableRandom rng;
    private final LongArrayBuffer uSortedNeighsId = new LongArrayBuffer();
    private double[] uSortedNeighsWeights;
    private final LongArrayBuffer vSortedNeighsId = new LongArrayBuffer();
    private double[] vSortedNeighsWeights;

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
        uSortedNeighsWeights = sortNeighsWithWeights(inputGraph, currentNode, uSortedNeighsId);
        do {
            candidateNode = getCandidateNode(uSortedNeighsId, uSortedNeighsWeights);
            vSortedNeighsWeights = sortNeighsWithWeights(inputGraph, candidateNode, vSortedNeighsId);
            var overlap = computeOverlapSimilarity(
                uSortedNeighsId,
                uSortedNeighsWeights,
                vSortedNeighsId,
                vSortedNeighsWeights
            );

            chanceOutOfNeighbours = 1.0D - overlap;
            q = rng.nextDouble();
        } while (q > chanceOutOfNeighbours);

        return candidateNode;
    }

    private double computeOverlapSimilarity(
        LongArrayBuffer neighsU,
        double[] weightsU,
        LongArrayBuffer neighsV,
        double[] weightsV
    ) {
        double similarity = OverlapSimilarity.computeWeightedSimilarity(
            neighsU.buffer,
            neighsV.buffer,
            weightsU,
            weightsV,
            neighsU.length,
            neighsV.length
        );
        if (Double.isNaN(similarity)) {
            return 0.0D;
        }
        return similarity;
    }

    private long getCandidateNode(LongArrayBuffer neighs, double[] weights) {
        var sumWeights = 0;
        for (int i = 0; i < neighs.length; i++)
            sumWeights += weights[i];
        var remainingMass = rng.nextDouble(0, sumWeights);

        int i = 0;
        while (remainingMass > 0) {
            remainingMass -= weights[i++];
        }

        return neighs.buffer[i - 1];
    }


    private static double[] sortNeighsWithWeights(Graph graph, long nodeId, LongArrayBuffer neighs) {
        var neighsCount = graph.degree(nodeId);
        neighs.ensureCapacity(neighsCount);
        neighs.length = neighsCount;
        var idx = new AtomicInteger(0);
        double[] weightsArray = new double[neighsCount];
        graph.forEachRelationship(nodeId, 0.0, (src, dst, w) -> {
            var localIdx = idx.getAndIncrement();
            neighs.buffer[localIdx] = dst;
            weightsArray[localIdx] = w;
            return true;
        });

        var weights = IntStream.range(0, weightsArray.length).boxed()
            .sorted((i, j) -> Long.compare(neighs.buffer[i], neighs.buffer[j]))
            .map(i -> weightsArray[i]).mapToDouble(x -> x).toArray();
        Arrays.sort(neighs.buffer, 0, neighsCount);
        return weights;
    }
}
