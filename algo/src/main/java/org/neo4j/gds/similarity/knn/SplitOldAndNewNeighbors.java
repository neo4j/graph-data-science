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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.core.concurrency.BiLongConsumer;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.SplittableRandom;

/**
 * First step of NN-Descent
 *
 * parallel for v ∈ V do
 *   old[v] ←− all items in B[v] with a false flag
 *   new[v] ←− ρK items in B[v] with a true flag
 *   Mark sampled items in B[v] as false;
 */
final class SplitOldAndNewNeighbors implements BiLongConsumer {
    private final SplittableRandom random;
    private final HugeObjectArray<NeighborList> neighbors;
    private final HugeObjectArray<LongArrayList> allOldNeighbors;
    private final HugeObjectArray<LongArrayList> allNewNeighbors;
    private final int sampledK;
    private final ProgressTracker progressTracker;

    SplitOldAndNewNeighbors(
        SplittableRandom random,
        HugeObjectArray<NeighborList> neighbors,
        HugeObjectArray<LongArrayList> allOldNeighbors,
        HugeObjectArray<LongArrayList> allNewNeighbors,
        int sampledK,
        ProgressTracker progressTracker
    ) {
        this.random = random;
        this.neighbors = neighbors;
        this.allOldNeighbors = allOldNeighbors;
        this.allNewNeighbors = allNewNeighbors;
        this.sampledK = sampledK;
        this.progressTracker = progressTracker;
    }

    @Override
    public void apply(long start, long end) {
        var rng = random.split();
        var sampledK = this.sampledK;
        var allNeighbors = this.neighbors;
        var allNewNeighbors = this.allNewNeighbors;
        var allOldNeighbors = this.allOldNeighbors;
        var sampled = new IntArrayList(sampledK);

        // TODO use cursors
        for (long nodeId = start; nodeId < end; nodeId++) {
            var neighbors = allNeighbors.get(nodeId);
            var k2 = neighbors.size();
            sampled.clear();
            LongArrayList oldNeighbors = null;

            for (int neighborIndex = 0, newNeighborCount = 0; neighborIndex < k2; neighborIndex++) {
                var neighborElement = neighbors.elementAt(neighborIndex);
                // incremental search, if we're already done with this node,
                // sort neighbor to old neighbors
                // we use the sign bit to keep track of the checked state of a node
                if (NeighborList.isChecked(neighborElement)) {
                    if (oldNeighbors == null) {
                        oldNeighbors = new LongArrayList();
                        allOldNeighbors.set(nodeId, oldNeighbors);
                    }
                    // unset the checked bit
                    var neighborNode = NeighborList.clearCheckedFlag(neighborElement);
                    oldNeighbors.add(neighborNode);
                } else {
                    // always start with the first `sampledK` elements
                    if (newNeighborCount < sampledK) {
                        sampled.add(neighborIndex);
                    } else {
                        // randomly replace earlier sampled nodes
                        var randomNode = rng.nextInt(newNeighborCount + 1);
                        if (randomNode < sampledK) {
                            sampled.set(randomNode, neighborIndex);
                        }
                    }
                    ++newNeighborCount;
                }
            }

            if (sampled.isEmpty()) {
                continue;
            }

            var newNeighbors = new LongArrayList();
            allNewNeighbors.set(nodeId, newNeighbors);

            for (var neighborIndex : sampled) {
                var neighborNode = neighbors.getAndFlagAsChecked(neighborIndex.value);
                assert nodeId != neighborNode;
                assert neighborNode >= 0;
                newNeighbors.add(neighborNode);
            }
        }
        progressTracker.logProgress(end - start);
    }
}
