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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.helper.FloatVectorTestUtils;

import java.util.Random;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Node2VecModelTest {

    @Test
    void testModel() {
        Random random = new Random(42);
        int numberOfClusters = 10;
        int clusterSize = 100;
        int numberOfWalks = 10;
        int walkLength = 80;

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            numberOfClusters * clusterSize,
            0.001,
            0.75,
            4
        );

        CompressedRandomWalks walks = generateRandomWalks(
            probabilitiesBuilder,
            numberOfClusters,
            clusterSize,
            numberOfWalks,
            walkLength,
            random
        );

        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig.builder()
            .embeddingDimension(10)
            .initialLearningRate(0.05)
            .negativeSamplingRate(1)
            .iterations(5)
            .concurrency(4)
            .build();

        int nodeCount = numberOfClusters * clusterSize;

        var node2VecModel = new Node2VecModel(
            nodeId -> nodeId,
            nodeCount,
            config,
            walks,
            probabilitiesBuilder.build(),
            ProgressTracker.NULL_TRACKER
        );

        var trainResult = node2VecModel.train();

        // as the order of the randomWalks is not deterministic, we also have non-fixed losses
        assertThat(trainResult.lossPerIteration())
            .hasSize(config.iterations())
            .allMatch(loss -> loss > 0 && Double.isFinite(loss));

        var embeddings = trainResult.embeddings();

        for (long idx = 0; idx < embeddings.size(); idx++) {
            assertThat(FloatVectorTestUtils.notContainsNaN(embeddings.get(idx))).isTrue();
        }

        double innerClusterSum = LongStream.range(0, numberOfClusters)
            .boxed()
            .flatMap(clusterId ->
                LongStream.range(clusterSize * clusterId, clusterSize * (clusterId + 1))
                    .boxed()
                    .flatMap(nodeId ->
                        LongStream.range(0, clusterSize)
                            .mapToObj(ignore -> {
                                var e1 = embeddings.get(nodeId).data();
                                var e2 = embeddings
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
                                var e1 = embeddings.get(nodeId).data();
                                var e2 = embeddings
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

    @Disabled("The order of the randomWalks + its usage in the training is not deterministic yet.")
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 4})
    void randomSeed(int iterations) {
        var random = new Random(42);
        int numberOfClusters = 10;
        int clusterSize = 100;
        int numberOfWalks = 10;
        int walkLength = 80;

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            numberOfClusters * clusterSize,
            0.001,
            0.75,
            4
        );

        CompressedRandomWalks walks = generateRandomWalks(probabilitiesBuilder, numberOfClusters, clusterSize, numberOfWalks, walkLength, random);

        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig.builder()
            .embeddingDimension(2)
            .initialLearningRate(0.05)
            .negativeSamplingRate(1)
            .randomSeed(1337L)
            .concurrency(4)
            .iterations(iterations)
            .build();

        int nodeCount = numberOfClusters * clusterSize;

        var node2VecModel = new Node2VecModel(
            nodeId -> nodeId,
            nodeCount,
            config,
            walks,
            probabilitiesBuilder.build(),
            ProgressTracker.NULL_TRACKER
        );

        var otherNode2VecModel = new Node2VecModel(
            nodeId -> nodeId,
            nodeCount,
            config,
            walks,
            probabilitiesBuilder.build(),
            ProgressTracker.NULL_TRACKER
        );

        var embeddings = node2VecModel.train().embeddings();
        var otherEmbeddings = otherNode2VecModel.train().embeddings();

        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            assertThat(embeddings.get(nodeId)).isEqualTo(otherEmbeddings.get(nodeId));
        }
    }

    private static CompressedRandomWalks generateRandomWalks(
        RandomWalkProbabilities.Builder probabilitiesBuilder,
        long numberOfClusters,
        int clusterSize,
        long numberOfWalks,
        long walkLength,
        Random random
    ) {
        var walks = new CompressedRandomWalks(
            numberOfClusters * clusterSize * numberOfWalks
        );

        for (long clusterId = 0; clusterId < numberOfClusters; clusterId++) {
            long finalClusterId = clusterId;
            long bound = clusterSize * (clusterId + 1);
            for (long nodeId = clusterSize * clusterId; nodeId < bound; nodeId++) {
                for (long ignore = 0; ignore < numberOfWalks; ignore++) {
                    long[] walk = LongStream.range(0, walkLength)
                        .map(foo -> random.nextInt(clusterSize) + (finalClusterId * clusterSize))
                        .toArray();

                    probabilitiesBuilder.registerWalk(walk);
                    walks.add(walk);
                }
            }
        }

        return walks;
    }
}
