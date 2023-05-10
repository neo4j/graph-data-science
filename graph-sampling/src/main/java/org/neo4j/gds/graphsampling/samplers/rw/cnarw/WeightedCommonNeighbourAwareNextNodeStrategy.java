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

import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.compress.DoubleArrayBuffer;
import org.neo4j.gds.api.compress.LongArrayBuffer;
import org.neo4j.gds.functions.similairty.OverlapSimilarity;
import org.neo4j.gds.graphsampling.samplers.rw.NextNodeStrategy;

import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class WeightedCommonNeighbourAwareNextNodeStrategy implements NextNodeStrategy {

    private final Graph inputGraph;
    private final SplittableRandom rng;
    private final LongArrayBuffer uSortedNeighsIds = new LongArrayBuffer();
    private final DoubleArrayBuffer uSortedNeighsWeights = new DoubleArrayBuffer();
    private final LongArrayBuffer vSortedNeighsIds = new LongArrayBuffer();
    private final DoubleArrayBuffer vSortedNeighsWeights = new DoubleArrayBuffer();

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
        sortNeighsWithWeights(inputGraph, currentNode, uSortedNeighsIds, uSortedNeighsWeights);
        do {
            candidateNode = getCandidateNode(uSortedNeighsIds, uSortedNeighsWeights);
            sortNeighsWithWeights(inputGraph, candidateNode, vSortedNeighsIds, vSortedNeighsWeights);
            var overlap = computeOverlapSimilarity(
                uSortedNeighsIds, uSortedNeighsWeights,
                vSortedNeighsIds, vSortedNeighsWeights
            );

            chanceOutOfNeighbours = 1.0D - overlap;
            q = rng.nextDouble();
        } while (q > chanceOutOfNeighbours);

        return candidateNode;
    }

    private double computeOverlapSimilarity(
        LongArrayBuffer neighsU, DoubleArrayBuffer weightsU,
        LongArrayBuffer neighsV, DoubleArrayBuffer weightsV
    ) {
        double similarityCutoff = 0.0d;
        double similarity = OverlapSimilarity.computeWeightedSimilarity(
            neighsU.buffer, neighsU.length,
            neighsV.buffer, neighsV.length,
            weightsU.buffer, weightsV.buffer,
            similarityCutoff
        );
        if (Double.isNaN(similarity)) {
            return 0.0D;
        }
        return similarity;
    }

    private long getCandidateNode(LongArrayBuffer neighs, DoubleArrayBuffer weights) {
        var sumWeights = 0;
        for (int i = 0; i < neighs.length; i++)
            sumWeights += weights.buffer[i];

        if (sumWeights == 0) {
            return rng.nextInt(neighs.length);
        }
        var remainingMass = rng.nextDouble(0, sumWeights);

        int i = 0;
        while (remainingMass > 0) {
            remainingMass -= weights.buffer[i++];
        }

        return neighs.buffer[i - 1];
    }

    private static void sortNeighsWithWeights(
        Graph graph, long nodeId,
        LongArrayBuffer neighs, DoubleArrayBuffer weights
    ) {
        var neighsCount = graph.degree(nodeId);

        neighs.ensureCapacity(neighsCount);
        neighs.length = neighsCount;

        weights.ensureCapacity(neighsCount);
        weights.length = neighsCount;

        var idx = new AtomicInteger(0);
        graph.forEachRelationship(nodeId, 0.0, (src, dst, w) -> {
            var localIdx = idx.getAndIncrement();
            neighs.buffer[localIdx] = dst;
            weights.buffer[localIdx] = w;
            return true;
        });

        sortDoubleArrayByLongValues(neighs.buffer, weights.buffer, neighsCount);
    }

    /**
     * Sort two arrays simultaneously based on values of the first (long) array.
     * E.g. {[4, 1, 8], [0.5, 1.9, 0.9]} -> {[1, 4, 8], [1,9, 0.5, 0.9]}
     *
     * @param longArray   Array of long values (e.g. neighbours ids)
     * @param doubleArray Array of double values (e.g. neighbours weighs)
     * @param length      Number of values to sort
     */
    private static void sortDoubleArrayByLongValues(long[] longArray, double[] doubleArray, int length) {
        assert longArray.length >= length;
        assert doubleArray.length >= length;
        if (longArrayIsSorted(longArray, length)) {
            return;
        }
        if (length < 10) {
            System.out.println("before");
            for (int i = 0; i < length; i++) {
                System.out.print(longArray[i] + ", ");
            }
            System.out.println();
            for (int i = 0; i < length; i++) {
                System.out.print(doubleArray[i] + ", ");
            }
            System.out.println();
        }
        var order = IndirectSort.mergesort(0, length, new AscendingLongComparator(longArray));
        for (int i = 0; i < length; i++) {
            swap(longArray, doubleArray, i, order[i]);
        }

        System.out.println("length = " + length);
        if (length < 10) {
            System.out.println("after");
            for (int i = 0; i < length; i++) {
                System.out.print(order[i] + ", ");
            }
            System.out.println();
            for (int i = 0; i < length; i++) {
                System.out.print(longArray[i] + ", ");
            }
            System.out.println();
            for (int i = 0; i < length; i++) {
                System.out.print(doubleArray[i] + ", ");
            }
            System.out.println();
        }
    }

    private static boolean longArrayIsSorted(long[] longArray, int length) {
        if (length <= 1) return true;
        for (int i = 0; i < length - 1; i++) {
            if (longArray[i] > longArray[i + 1]) {
                return false;
            }
        }
        return true;
    }

    public static class AscendingLongComparator implements IndirectComparator {
        private final long[] array;

        public AscendingLongComparator(long[] array) {
            this.array = array;
        }

        public int compare(int indexA, int indexB) {
            final long a = array[indexA];
            final long b = array[indexB];

            if (a < b)
                return -1;
            if (a > b)
                return 1;
            return 0;
        }
    }

    private static void swap(long[] l, double[] d, int i, int j) {
        long tempLong = l[i];
        l[i] = l[j];
        l[j] = tempLong;

        double tempDouble = d[i];
        d[i] = d[j];
        d[j] = tempDouble;
    }
}
