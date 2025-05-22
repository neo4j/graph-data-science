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

import org.agrona.collections.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class Node2VecProgressTrackingTest {

    private static final List<Long> NO_SOURCE_NODES = List.of();

    private static final Graph graph = new RandomGraphGeneratorBuilder()
        .nodeCount(300)
        .relationshipDistribution(RelationshipDistribution.RANDOM)
        .averageDegree(15)
        .seed(19L)
        .build()
        .generate();


    @Test
    void iterationLoggingShouldNotHang() {
        var concurrency = 8;
        var walkParameters = new SamplingWalkParameters(
            NO_SOURCE_NODES,
            50,
            80,
            2.0,
            0.5,
            0.001,
            0.75,
            1000
        );

        var trainParameters = new TrainParameters(
            0.025,
            0.0001,
            1,
            10,
            7,
            128,
            EmbeddingInitializer.NORMALIZED
        );


        var parameters = new Node2VecParameters(
            walkParameters,
            trainParameters,
            new Concurrency(concurrency),
            Optional.of(1337L)
        );

        var lazyMock = mock(Log.class);
        var iteration1reached100At = new MutableLong();
        doAnswer(invocation -> {
            var infoMessage = invocation.getArgument(0, String.class);
            if (infoMessage.contains("iteration 1 of 1 100%")){
                iteration1reached100At.set(System.currentTimeMillis());
            }
            return  null;
            }).when(lazyMock).info(anyString());
        var finishedIteration1At = new MutableLong();
        doAnswer(invocation -> {
            var task = invocation.getArgument(2, String.class);
            var infoMessage = invocation.getArgument(3, String.class);
            if (task.contains("iteration 1 of 1") && infoMessage.contains("Finished")){
                finishedIteration1At.set(System.currentTimeMillis());
            }
            return null;
        }).when(lazyMock).info(anyString(),anyString(),anyString(),anyString());

        var progressTracker = new TestProgressTracker(
            Node2VecTask.create(graph, parameters),
            new LoggerForProgressTrackingAdapter(lazyMock),
            new Concurrency(concurrency),
            EmptyTaskRegistryFactory.INSTANCE
        );

        try(var ignored = Executors.newFixedThreadPool(concurrency)) {
             Node2Vec.create(
                graph,
                parameters,
                progressTracker,
                TerminationFlag.RUNNING_TRUE
            ).compute().embeddings();
        }
        assertThat((finishedIteration1At.longValue() - iteration1reached100At.longValue())).isLessThanOrEqualTo(700);

    }
}
