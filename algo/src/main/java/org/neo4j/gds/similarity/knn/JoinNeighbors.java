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
package org.neo4j.gds.similarity.knn;

import com.carrotsearch.hppc.LongArrayList;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.SplittableRandom;

final class JoinNeighbors implements Runnable {
    private final SplittableRandom random;
    private final SimilarityFunction similarityFunction;
    private final NeighborFilter neighborFilter;
    private final Neighbors allNeighbors;
    private final HugeObjectArray<LongArrayList> allOldNeighbors;
    private final HugeObjectArray<LongArrayList> allNewNeighbors;
    private final HugeObjectArray<LongArrayList> allReverseOldNeighbors;
    private final HugeObjectArray<LongArrayList> allReverseNewNeighbors;
    private final int sampledK;
    private final int randomJoins;
    private final ProgressTracker progressTracker;
    private final long nodeCount;
    private final Partition partition;
    private final double perturbationRate;
    private long updateCount;


    JoinNeighbors(
        SplittableRandom random,
        SimilarityFunction similarityFunction,
        NeighborFilter neighborFilter,
        Neighbors allNeighbors,
        HugeObjectArray<LongArrayList> allOldNeighbors,
        HugeObjectArray<LongArrayList> allNewNeighbors,
        HugeObjectArray<LongArrayList> allReverseOldNeighbors,
        HugeObjectArray<LongArrayList> allReverseNewNeighbors,
        int sampledK,
        double perturbationRate,
        int randomJoins,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.random = random;
        this.similarityFunction = similarityFunction;
        this.neighborFilter = neighborFilter;
        this.allNeighbors = allNeighbors;
        this.nodeCount = allNewNeighbors.size();
        this.allOldNeighbors = allOldNeighbors;
        this.allNewNeighbors = allNewNeighbors;
        this.allReverseOldNeighbors = allReverseOldNeighbors;
        this.allReverseNewNeighbors = allReverseNewNeighbors;
        this.sampledK = sampledK;
        this.randomJoins = randomJoins;
        this.partition = partition;
        this.progressTracker = progressTracker;
        this.perturbationRate = perturbationRate;
        this.updateCount = 0;
    }

    @Override
    public void run() {
        var startNode = partition.startNode();
        long endNode = startNode + partition.nodeCount();

        for (long nodeId = startNode; nodeId < endNode; nodeId++) {
            // old[v] ∪ Sample(old′[v], ρK)
            var oldNeighbors = allOldNeighbors.get(nodeId);
            if (oldNeighbors != null) {
                combineNeighbors(allReverseOldNeighbors.get(nodeId), oldNeighbors);
            }


            // new[v] ∪ Sample(new′[v], ρK)
            var newNeighbors = allNewNeighbors.get(nodeId);
            if (newNeighbors != null) {
                combineNeighbors(allReverseNewNeighbors.get(nodeId), newNeighbors);

                this.updateCount += joinNewNeighbors(nodeId, oldNeighbors, newNeighbors);
            }

            // this isn't in the paper
            randomJoins(nodeCount, nodeId);
        }
        progressTracker.logProgress(partition.nodeCount());
    }

    private long joinNewNeighbors(
        long nodeId, LongArrayList oldNeighbors, LongArrayList newNeighbors
    ) {
        long updateCount = 0;

        var newNeighborElements = newNeighbors.buffer;
        var newNeighborsCount = newNeighbors.elementsCount;
        boolean similarityIsSymmetric = similarityFunction.isSymmetric();

        for (int i = 0; i < newNeighborsCount; i++) {
            var elem1 = newNeighborElements[i];
            assert elem1 != nodeId;

            // join(u1, nodeId), this isn't in the paper
            updateCount += join(elem1, nodeId);

            //  try out using the new neighbors between themselves / join(new_nbd, new_ndb)
            for (int j = i + 1; j < newNeighborsCount; j++) {
                var elem2 = newNeighborElements[j];
                if (elem1 == elem2) {
                    continue;
                }

                if (similarityIsSymmetric) {
                    updateCount += joinSymmetric(elem1, elem2);
                } else {
                    updateCount += join(elem1, elem2);
                    updateCount += join(elem2, elem1);
                }
            }

            // try out joining the old neighbors with the new neighbor / join(new_nbd, old_ndb)
            if (oldNeighbors != null) {
                for (var oldElemCursor : oldNeighbors) {
                    var elem2 = oldElemCursor.value;

                    if (elem1 == elem2) {
                        continue;
                    }

                    if (similarityIsSymmetric) {
                        updateCount += joinSymmetric(elem1, elem2);
                    } else {
                        updateCount += join(elem1, elem2);
                        updateCount += join(elem2, elem1);
                    }
                }
            }
        }
        return updateCount;
    }

    private void combineNeighbors(@Nullable LongArrayList reversedNeighbors, LongArrayList neighbors) {
        if (reversedNeighbors != null) {
            var numberOfReverseNeighbors = reversedNeighbors.size();
            for (var elem : reversedNeighbors) {
                if (random.nextInt(numberOfReverseNeighbors) < sampledK) {
                    // TODO: this could add nodes twice, maybe? should this be a set?
                    neighbors.add(elem.value);
                }
            }
        }
    }

    private void randomJoins(long nodeCount, long nodeId) {
        for (int i = 0; i < randomJoins; i++) {
            var randomNodeId = random.nextLong(nodeCount - 1);
            // shifting the randomNode as the randomNode was picked from [0, n-1)
            if (randomNodeId >= nodeId) {
                ++randomNodeId;
            }
            // random joins are not counted towards the actual update counter
            join(nodeId, randomNodeId);
        }
    }

    private long joinSymmetric(long node1, long node2) {
        assert node1 != node2;

        if (neighborFilter.excludeNodePair(node1, node2)) {
            return 0;
        }

        var similarity = similarityFunction.computeSimilarity(node1, node2);

        var neighbors1 = allNeighbors.getAndIncrementCounter(node1);

        var updates = 0L;

        synchronized (neighbors1) {
            updates += neighbors1.add(node2, similarity, random, perturbationRate);
        }

        var neighbors2 = allNeighbors.get(node2);

        synchronized (neighbors2) {
            updates += neighbors2.add(node1, similarity, random, perturbationRate);
        }

        return updates;
    }

    private long join(long node1, long node2) {
        assert node1 != node2;

        if (neighborFilter.excludeNodePair(node1, node2)) {
            return 0;
        }

        var similarity = similarityFunction.computeSimilarity(node1, node2);
        var neighbors = allNeighbors.getAndIncrementCounter(node1);

        synchronized (neighbors) {
            return neighbors.add(node2, similarity, random, perturbationRate);
        }
    }

    long nodePairsConsidered() {
        return allNeighbors.joinCounter();
    }

    long updateCount() {
        return updateCount;
    }
}
