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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;

import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class JoinNeighborsTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a { knn: 1.2, prop: 1.0 } )" +
            ", (b { knn: 1.1, prop: 5.0 } )" +
            ", (c { knn: 42.0, prop: 10.0 } )";
    @Inject
    private TestGraph graph;

    @Test
    void joinNeighbors() {
        NeighbourConsumer neighbourConsumer = NeighbourConsumer.EMPTY_CONSUMER;
        SplittableRandom random = new SplittableRandom(42);
        double perturbationRate = 0.0;
        var allNeighbors = HugeObjectArray.of(
            new NeighborList(1, neighbourConsumer),
            new NeighborList(1, neighbourConsumer),
            new NeighborList(1, neighbourConsumer)
        );
        // setting an artificial priority to assure they will be replaced
        allNeighbors.get(0).add(1, 0.0, random, perturbationRate);
        allNeighbors.get(1).add(2, 0.0, random, perturbationRate);
        allNeighbors.get(2).add(0, 0.0, random, perturbationRate);

        var allNewNeighbors = HugeObjectArray.of(
            LongArrayList.from(1, 2),
            null,
            null
        );

        var allOldNeighbors = HugeObjectArray.newArray(LongArrayList.class, graph.nodeCount());

        SimilarityFunction similarityFunction = new SimilarityFunction(new SimilarityComputer() {
            @Override
            public double similarity(long firstNodeId, long secondNodeId) {
                return ((double) secondNodeId) / (firstNodeId + secondNodeId);
            }

            @Override
            public boolean isSymmetric() {
                return true;
            }
        });

        var joinNeighbors = new JoinNeighbors(
            Partition.of(0, 1),
            new Neighbors(allNeighbors),
            allOldNeighbors,
            allNewNeighbors,
            HugeObjectArray.newArray(LongArrayList.class, graph.nodeCount()),
            HugeObjectArray.newArray(LongArrayList.class, graph.nodeCount()),
            new KnnNeighborFilter(graph.nodeCount()),
            similarityFunction,
            1,
            perturbationRate,
            0,
            random,
            // simplifying the test by only running over a single node
            ProgressTracker.NULL_TRACKER
        );

        joinNeighbors.run();

        // 1-0, 2-0, 1-2/2-1
        assertThat(joinNeighbors.nodePairsConsidered()).isEqualTo(3);

        assertThat(allNeighbors.get(0).elements()).containsExactly(1L);
        assertThat(allNeighbors.get(1).elements()).containsExactly(2L);
        // this gets updated due to joining the new neighbors together
        assertThat(allNeighbors.get(2).elements()).containsExactly(1L);
    }
}
