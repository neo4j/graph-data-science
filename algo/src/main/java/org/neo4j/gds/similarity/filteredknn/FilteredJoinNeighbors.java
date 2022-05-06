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
package org.neo4j.gds.similarity.filteredknn;

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;

import java.util.SplittableRandom;

final class FilteredJoinNeighbors implements Runnable {
    private final SplittableRandom random;
    private final SimilarityComputer computer;
    private final FilteredNeighborFilter neighborFilter;
    private final HugeObjectArray<FilteredNeighborList> neighbors;
    private final HugeObjectArray<LongArrayList> allOldNeighbors;
    private final HugeObjectArray<LongArrayList> allNewNeighbors;
    private final HugeObjectArray<LongArrayList> allReverseOldNeighbors;
    private final HugeObjectArray<LongArrayList> allReverseNewNeighbors;
    private final long n;
    private final int k;
    private final int sampledK;
    private final int randomJoins;
    private final ProgressTracker progressTracker;
    private long updateCount;
    private final Partition partition;
    private long nodePairsConsidered;
    private final double perturbationRate;

    FilteredJoinNeighbors(
        SplittableRandom random,
        SimilarityComputer computer,
        FilteredNeighborFilter neighborFilter,
        HugeObjectArray<FilteredNeighborList> neighbors,
        HugeObjectArray<LongArrayList> allOldNeighbors,
        HugeObjectArray<LongArrayList> allNewNeighbors,
        HugeObjectArray<LongArrayList> allReverseOldNeighbors,
        HugeObjectArray<LongArrayList> allReverseNewNeighbors,
        long n,
        int k,
        int sampledK,
        double perturbationRate,
        int randomJoins,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.random = random;
        this.computer = computer;
        this.neighborFilter = neighborFilter;
        this.neighbors = neighbors;
        this.allOldNeighbors = allOldNeighbors;
        this.allNewNeighbors = allNewNeighbors;
        this.allReverseOldNeighbors = allReverseOldNeighbors;
        this.allReverseNewNeighbors = allReverseNewNeighbors;
        this.n = n;
        this.k = k;
        this.sampledK = sampledK;
        this.randomJoins = randomJoins;
        this.partition = partition;
        this.progressTracker = progressTracker;
        this.perturbationRate = perturbationRate;
        this.updateCount = 0;
        this.nodePairsConsidered = 0;
    }

    @Override
    public void run() {
        var rng = random;
        var computer = this.computer;
        var n = this.n;
        var k = this.k;
        var sampledK = this.sampledK;
        var allNeighbors = this.neighbors;
        var allNewNeighbors = this.allNewNeighbors;
        var allOldNeighbors = this.allOldNeighbors;
        var allReverseNewNeighbors = this.allReverseNewNeighbors;
        var allReverseOldNeighbors = this.allReverseOldNeighbors;

        var startNode = partition.startNode();
        long endNode = startNode + partition.nodeCount();

        for (long nodeId = startNode; nodeId < endNode; nodeId++) {
            // old[v] ∪ Sample(old′[v], ρK)
            var oldNeighbors = allOldNeighbors.get(nodeId);
            if (oldNeighbors != null) {
                joinOldNeighbors(rng, sampledK, allReverseOldNeighbors, nodeId, oldNeighbors);
            }


            // new[v] ∪ Sample(new′[v], ρK)
            var newNeighbors = allNewNeighbors.get(nodeId);
            if (newNeighbors != null) {
                this.updateCount += joinNewNeighbors(
                    rng,
                    computer,
                    n,
                    k,
                    sampledK,
                    allNeighbors,
                    allReverseNewNeighbors,
                    nodeId,
                    oldNeighbors,
                    newNeighbors
                );
            }

            // this isn't in the paper
            randomJoins(rng, computer, n, k, allNeighbors, nodeId, this.randomJoins);
        }
        progressTracker.logProgress(partition.nodeCount());
    }

    long updateCount() {
        return updateCount;
    }

    private void joinOldNeighbors(
        SplittableRandom rng,
        int sampledK,
        HugeObjectArray<LongArrayList> allReverseOldNeighbors,
        long nodeId,
        LongArrayList oldNeighbors
    ) {
        var reverseOldNeighbors = allReverseOldNeighbors.get(nodeId);
        if (reverseOldNeighbors != null) {
            var numberOfReverseOldNeighbors = reverseOldNeighbors.size();
            for (var elem : reverseOldNeighbors) {
                if (rng.nextInt(numberOfReverseOldNeighbors) < sampledK) {
                    // TODO: this could add nodes twice, maybe? should this be a set?
                    oldNeighbors.add(elem.value);
                }
            }
        }
    }

    private long joinNewNeighbors(
        SplittableRandom rng,
        SimilarityComputer computer,
        long n,
        int k,
        int sampledK,
        HugeObjectArray<FilteredNeighborList> allNeighbors,
        HugeObjectArray<LongArrayList> allReverseNewNeighbors,
        long nodeId,
        LongArrayList oldNeighbors,
        LongArrayList newNeighbors
    ) {
        long updateCount = 0;

        joinOldNeighbors(rng, sampledK, allReverseNewNeighbors, nodeId, newNeighbors);

        var newNeighborElements = newNeighbors.buffer;
        var newNeighborsCount = newNeighbors.elementsCount;

        for (int i = 0; i < newNeighborsCount; i++) {
            var elem1 = newNeighborElements[i];
            assert elem1 != nodeId;

            // join(u1, v), this isn't in the paper
            updateCount += join(rng, computer, allNeighbors, n, k, elem1, nodeId);

            // join(new_nbd, new_ndb)
            for (int j = i + 1; j < newNeighborsCount; j++) {
                var elem2 = newNeighborElements[i];
                if (elem1 == elem2) {
                    continue;
                }

                updateCount += join(rng, computer, allNeighbors, n, k, elem1, elem2);
                updateCount += join(rng, computer, allNeighbors, n, k, elem2, elem1);
            }

            // join(new_nbd, old_ndb)
            if (oldNeighbors != null) {
                for (var oldElemCursor : oldNeighbors) {
                    var elem2 = oldElemCursor.value;

                    if (elem1 == elem2) {
                        continue;
                    }

                    updateCount += join(rng, computer, allNeighbors, n, k, elem1, elem2);
                    updateCount += join(rng, computer, allNeighbors, n, k, elem2, elem1);
                }
            }
        }
        return updateCount;
    }

    private void randomJoins(
        SplittableRandom rng,
        SimilarityComputer computer,
        long n,
        int k,
        HugeObjectArray<FilteredNeighborList> allNeighbors,
        long nodeId,
        int randomJoins
    ) {
        for (int i = 0; i < randomJoins; i++) {
            var randomNodeId = rng.nextLong(n - 1);
            if (randomNodeId >= nodeId) {
                ++randomNodeId;
            }
            // random joins are not counted towards the actual update counter
            join(rng, computer, allNeighbors, n, k, nodeId, randomNodeId);
        }
    }

    private long join(
        SplittableRandom splittableRandom,
        SimilarityComputer computer,
        HugeObjectArray<FilteredNeighborList> allNeighbors,
        long n,
        int k,
        long base,
        long joiner
    ) {
        assert base != joiner;
        assert n > 1 && k > 0;

        if (neighborFilter.excludeNodePair(base, joiner)) {
            return 0;
        }

        var similarity = computer.safeSimilarity(base, joiner);
        nodePairsConsidered++;
        var neighbors = allNeighbors.get(base);

        synchronized (neighbors) {
            var k2 = neighbors.size();

            assert k2 > 0;
            assert k2 <= k;
            assert k2 <= n - 1;

            return neighbors.add(joiner, similarity, splittableRandom, perturbationRate);
        }
    }

    long nodePairsConsidered() {
        return nodePairsConsidered;
    }
}
