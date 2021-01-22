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
package org.neo4j.gds.embeddings.node2vec;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Random;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Node2VecModelTest {

    @Test
    void testModel() {
        Random random = new Random(42);
        int numberOfClusters = 10;
        int clusterSize = 100;
        int numberOfWalks = 10;
        int walkLength = 80;

        MutableLong counter = new MutableLong(0);
        var walks = HugeObjectArray.newArray(
            long[].class,
            numberOfClusters * clusterSize * numberOfWalks,
            AllocationTracker.empty()
        );
        LongStream.range(0, numberOfClusters)
            .boxed()
            .flatMap(clusterId ->
                LongStream.range(clusterSize * clusterId, clusterSize * (clusterId + 1))
                    .boxed()
                    .flatMap(nodeId ->
                        LongStream.range(0, numberOfWalks)
                            .mapToObj(ignore ->
                                LongStream.range(0, walkLength)
                                    .map(foo -> random.nextInt(clusterSize) + (clusterId * clusterSize))
                                    .toArray()
                            )
                    )
            ).forEach(walk -> walks.set(counter.getAndIncrement(), walk));


        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig.builder()
            .embeddingDimension(10)
            .initialLearningRate(0.05)
            .negativeSamplingRate(1)
            .concurrency(4)
            .build();

        int nodeCount = numberOfClusters * clusterSize;
        Node2VecModel word2Vec = new Node2VecModel(
            nodeCount,
            config,
            walks,
            new ProbabilityComputer(walks, nodeCount, 0.001, 0.75, 4, AllocationTracker.empty()),
            TestProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );

        word2Vec.train();

        double innerClusterSum = LongStream.range(0, numberOfClusters)
            .boxed()
            .flatMap(clusterId ->
                LongStream.range(clusterSize * clusterId, clusterSize * (clusterId + 1))
                    .boxed()
                    .flatMap(nodeId ->
                        LongStream.range(0, clusterSize)
                            .mapToObj(ignore -> {
                                var e1 = word2Vec.getEmbeddings().get(nodeId).data();
                                var e2 = word2Vec
                                    .getEmbeddings()
                                    .get(random.nextInt(clusterSize) + (clusterId * clusterSize))
                                    .data();
                                return Intersections.cosine(e1, e2, e1.length);
                            })
                    )
            ).mapToDouble(foo -> foo)
            .sum();

        assertEquals(
            1,
            innerClusterSum / (numberOfClusters * clusterSize * clusterSize),
            0.05,
            "Average inner-cluster similarity should be about 1"
        );

        var extraClusterSum = LongStream.range(0, numberOfClusters)
            .boxed()
            .flatMap(clusterId ->
                LongStream.range(clusterSize * clusterId, clusterSize * (clusterId + 1))
                    .boxed()
                    .flatMap(nodeId ->
                        LongStream.range(0, clusterSize)
                            .mapToObj(ignore -> {
                                long otherClusterId = (clusterId + random.nextInt(numberOfClusters - 1) + 1) % numberOfClusters;
                                var e1 = word2Vec.getEmbeddings().get(nodeId).data();
                                var e2 = word2Vec
                                    .getEmbeddings()
                                    .get(random.nextInt(clusterSize) + (otherClusterId * clusterSize))
                                    .data();
                                return Intersections.cosine(e1, e2, e1.length);
                            })
                    )
            ).mapToDouble(foo -> foo)
            .sum();

        assertEquals(
            0.35,
            extraClusterSum / (numberOfClusters * clusterSize * clusterSize),
            0.05,
            "Average extra-cluster similarity should be about 0.35"
        );
    }
}
