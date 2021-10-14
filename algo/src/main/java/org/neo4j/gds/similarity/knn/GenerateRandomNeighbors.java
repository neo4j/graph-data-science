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

import com.carrotsearch.hppc.LongHashSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.SplittableRandom;

/**
 * Initial step in KNN calculation.
 */
final class GenerateRandomNeighbors implements Runnable {
    private final SplittableRandom random;
    private final SimilarityComputer computer;
    private final HugeObjectArray<NeighborList> neighbors;
    private final long n;
    private final int k;
    private final int k2;
    private final ProgressTracker progressTracker;
    private final Partition partition;

    GenerateRandomNeighbors(
        SplittableRandom random,
        SimilarityComputer computer,
        HugeObjectArray<NeighborList> neighbors,
        long n,
        int k,
        int k2,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.random = random;
        this.computer = computer;
        this.neighbors = neighbors;
        this.n = n;
        this.k = k;
        this.k2 = k2;
        this.progressTracker = progressTracker;
        this.partition = partition;
    }

    @Override
    public void run() {
        var rng = random.split();
        var computer = this.computer;
        var n = this.n;
        var k = this.k;
        var k2 = this.k2;
        var chosen = new LongHashSet(k2);

        partition.consume(nodeId -> {
            chosen.clear();

            for (int i = 0; i < k2; i++) {
                var randomNode = rng.nextLong(n - 1);
                if (randomNode >= nodeId) {
                    ++randomNode;
                }
                assert nodeId != randomNode;
                chosen.add(randomNode);
            }
            assert chosen.size() <= k2;

            var neighbors = new NeighborList(k);
            for (var chosenCursor : chosen) {
                var neighborNode = chosenCursor.value;
                assert nodeId != neighborNode;
                var similarity = computer.safeSimilarity(nodeId, neighborNode);
                neighbors.add(neighborNode, similarity, rng);
            }

            assert neighbors.size() > 0; // because K > 0 and N > 1
            assert neighbors.size() <= k;

            progressTracker.logProgress();
            this.neighbors.set(nodeId, neighbors);
        });
    }
}
