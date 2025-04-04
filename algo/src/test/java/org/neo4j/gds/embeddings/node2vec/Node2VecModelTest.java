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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.Random;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SoftAssertionsExtension.class)
class Node2VecModelTest {

    @InjectSoftAssertions
    private SoftAssertions assertions;

    @Test
    void testModel() {
        var random = new Random(42);
        int numberOfClusters = 10;
        int clusterSize = 100;
        int numberOfWalks = 10;
        int walkLength = 80;

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            numberOfClusters * clusterSize,
            new Concurrency(4),
            0.001,
            0.75
        );

        var walks = generateRandomWalks(
            probabilitiesBuilder,
            numberOfClusters,
            clusterSize,
            numberOfWalks,
            walkLength,
            random
        );

        var embeddingDimension = 10;
        var trainParameters = new TrainParameters(0.05, 0.0001, 5, 10, 1, embeddingDimension, EmbeddingInitializer.NORMALIZED);

        int nodeCount = numberOfClusters * clusterSize;

        var node2VecModel = new Node2VecModel(
            nodeId -> nodeId,
            nodeCount,
            trainParameters,
            new Concurrency(4),
            Optional.empty(),
            walks,
            probabilitiesBuilder.build(),
            ProgressTracker.NULL_TRACKER
        );

        var trainResult = node2VecModel.train();

        // as the order of the randomWalks is not deterministic, we also have non-fixed losses
        assertThat(trainResult.lossPerIteration())
            .hasSize(5)
            .allSatisfy(loss -> assertThat(loss).isPositive().isFinite());

        var embeddings = trainResult.embeddings();
        assertThat(embeddings.size()).isEqualTo(nodeCount);

        for (long idx = 0; idx < nodeCount; idx++) {
            assertThat(embeddings.get(idx).data())
                .hasSize(embeddingDimension)
                .doesNotContain(Float.NaN);
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

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 4})
    @DisplayName("Should produce the same embeddings for the same randomSeed and single-threaded.")
    void twoRunsSingleThreadedWithTheSameRandomSeed(int iterations) {
        var random = new Random(42);
        int numberOfClusters = 10;
        int clusterSize = 100;
        int numberOfWalks = 10;
        int walkLength = 80;

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            numberOfClusters * clusterSize,
            new Concurrency(4),
            0.001,
            0.75
        );

        CompressedRandomWalks walks = generateRandomWalks(probabilitiesBuilder, numberOfClusters, clusterSize, numberOfWalks, walkLength, random);

        var trainParameters = new TrainParameters(0.05, 0.0001, iterations, 10, 1, 2, EmbeddingInitializer.NORMALIZED);

        int nodeCount = numberOfClusters * clusterSize;

        var firstRunEmbeddings = new Node2VecModel(
            nodeId -> nodeId,
            nodeCount,
            trainParameters,
            new Concurrency(1),
            Optional.of(1337L),
            walks,
            probabilitiesBuilder.build(),
            ProgressTracker.NULL_TRACKER
        ).train().embeddings();

        var secondRunEmbedding = new Node2VecModel(
            nodeId -> nodeId,
            nodeCount,
            trainParameters,
            new Concurrency(1),
            Optional.of(1337L),
            walks,
            probabilitiesBuilder.build(),
            ProgressTracker.NULL_TRACKER
        ).train().embeddings();

        for (long node = 0; node < nodeCount; node++) {
            var e1 = firstRunEmbeddings.get(node).data();
            var e2 = secondRunEmbedding.get(node).data();
            assertThat(e1)
                .isEqualTo(e2);
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
        int index = 0;
        for (long clusterId = 0; clusterId < numberOfClusters; clusterId++) {
            long finalClusterId = clusterId;
            long bound = clusterSize * (clusterId + 1);
            for (long nodeId = clusterSize * clusterId; nodeId < bound; nodeId++) {
                for (long ignore = 0; ignore < numberOfWalks; ignore++) {
                    long[] walk = LongStream.range(0, walkLength)
                        .map(foo -> random.nextInt(clusterSize) + (finalClusterId * clusterSize))
                        .toArray();

                    probabilitiesBuilder.registerWalk(walk);
                    walks.add(index++, walk);
                }
            }
        }
        walks.setSize(index);
        walks.setMaxWalkLength((int) walkLength);

        return walks;
    }
}
