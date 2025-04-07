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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class Node2VecModelTest {

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

        CompressedRandomWalks walks = generateRandomWalks(
            probabilitiesBuilder,
            numberOfClusters,
            clusterSize,
            numberOfWalks,
            walkLength,
            random
        );

        var trainParameters = new TrainParameters(0.05, 0.0001, 5, 10, 1, 10, EmbeddingInitializer.NORMALIZED);

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
                .hasSize(trainParameters.embeddingDimension())
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

   // @Disabled("The order of the randomWalks + its usage in the training is not deterministic yet.")
    //We can only guarantee consstency for concurrency 1
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
            new Concurrency(4),
            0.001,
            0.75
        );

        CompressedRandomWalks walks = generateRandomWalks(probabilitiesBuilder, numberOfClusters, clusterSize, numberOfWalks, walkLength, random);

        var trainParameters = new TrainParameters(0.05, 0.0001, iterations, 10, 1, 2, EmbeddingInitializer.NORMALIZED);

        int nodeCount = numberOfClusters * clusterSize;

        var node2VecModel = new Node2VecModel(
            nodeId -> nodeId,
            nodeCount,
            trainParameters,
            new Concurrency(1),
            Optional.of(1337L),
            walks,
            probabilitiesBuilder.build(),
            ProgressTracker.NULL_TRACKER
        );

        var otherNode2VecModel = new Node2VecModel(
            nodeId -> nodeId,
            nodeCount,
            trainParameters,
            new Concurrency(1),
            Optional.of(1337L),
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

    @Test
    @DisplayName("When creating multiple tasks with random seed the actual seed for the task should be `randomSeed + taskId`.")
    void shouldCreateTrainingTasksWithCorrectRandomSeed() {
        var randomWalksMock = mock(CompressedRandomWalks.class);
        when(randomWalksMock.size()).thenReturn(10L);
        when(randomWalksMock.walkLength(anyLong())).thenReturn(3);

        var randomWalkProbabilitiesMock = mock(RandomWalkProbabilities.class);
        when(randomWalkProbabilitiesMock.sampleCount()).thenReturn(30L);
        when(randomWalkProbabilitiesMock.negativeSamplingDistribution()).thenReturn(HugeLongArray.newArray(10));

        var trainParametersMock = mock(TrainParameters.class);
        when(trainParametersMock.embeddingInitializer()).thenReturn(EmbeddingInitializer.UNIFORM);

        var node2VecModel = spy(
            new Node2VecModel(
                LongUnaryOperator.identity(),
                1000,
                trainParametersMock,
                new Concurrency(4),
                Optional.of(1L), // Random Seed
                randomWalksMock,
                randomWalkProbabilitiesMock,
                ProgressTracker.NULL_TRACKER
            )
        );

        var taskIdTracker = new AtomicInteger(0);
        var trainingTasks = node2VecModel.createTrainingTasks(0.2f, taskIdTracker);

        assertThat(trainingTasks).hasSize(5);

        verify(node2VecModel, times(5)).createPositiveSampleProducer(any(), anyLong());
        verify(node2VecModel, times(1)).createPositiveSampleProducer(any(), eq(1L));
        verify(node2VecModel, times(1)).createPositiveSampleProducer(any(), eq(2L));
        verify(node2VecModel, times(1)).createPositiveSampleProducer(any(), eq(3L));
        verify(node2VecModel, times(1)).createPositiveSampleProducer(any(), eq(4L));
        verify(node2VecModel, times(1)).createPositiveSampleProducer(any(), eq(5L));

        verify(node2VecModel, times(5)).createNegativeSampleProducer(anyLong());
        verify(node2VecModel, times(1)).createNegativeSampleProducer(1L);
        verify(node2VecModel, times(1)).createNegativeSampleProducer(2L);
        verify(node2VecModel, times(1)).createNegativeSampleProducer(3L);
        verify(node2VecModel, times(1)).createNegativeSampleProducer(4L);
        verify(node2VecModel, times(1)).createNegativeSampleProducer(5L);
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
