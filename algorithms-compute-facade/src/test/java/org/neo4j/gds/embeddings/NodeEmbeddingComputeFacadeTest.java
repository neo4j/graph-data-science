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
package org.neo4j.gds.embeddings;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.embeddings.node2vec.EmbeddingInitializer;
import org.neo4j.gds.embeddings.node2vec.Node2VecParameters;
import org.neo4j.gds.embeddings.node2vec.SamplingWalkParameters;
import org.neo4j.gds.embeddings.node2vec.TrainParameters;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.ml.core.tensor.FloatVector;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NodeEmbeddingComputeFacadeTest {
    private static final Optional<Long> NO_RANDOM_SEED = Optional.empty();
    private static final List<Long> NO_SOURCE_NODES = List.of();

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ProgressTrackerFactory progressTrackerFactoryMock;
    @Mock
    private ProgressTracker progressTrackerMock;

    @Mock
    private JobId jobIdMock;

    @Mock
    private Log logMock;

    private NodeEmbeddingComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(progressTrackerMock);

        facade = new NodeEmbeddingComputeFacade(
            Log.noOpLog(),
            new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock),
            progressTrackerFactoryMock,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Nested
    @GdlExtension
    class Node2VecTest {

        @GdlGraph(graphNamePrefix = "node2Vec")
        private static final String NODE_2_VEC =
            """
                    CREATE
                      (a:Node1),
                      (b:Node1),
                      (c:Node2),
                      (d:Isolated),
                      (e:Isolated),
                      (a)-[:REL {prop: 1.0}]->(b),
                      (b)-[:REL {prop: 1.0}]->(a),
                      (a)-[:REL {prop: 1.0}]->(c),
                      (c)-[:REL {prop: 1.0}]->(a),
                      (b)-[:REL {prop: 1.0}]->(c),
                      (c)-[:REL {prop: 1.0}]->(b)
                """;


        @Inject
        private Graph node2VecGraph;

        @Test
        void node2Vec() {
            int embeddingDimension = 128;

            var trainParameters = new TrainParameters(
                0.025,
                0.0001,
                1,
                10,
                5,
                embeddingDimension,
                EmbeddingInitializer.NORMALIZED
            );

            var samplingWalkParameters = new SamplingWalkParameters(
                NO_SOURCE_NODES,
                10,
                80,
                1.0,
                1.0,
                0.001,
                0.75,
                1000
            );

            var parameters = new Node2VecParameters(
                samplingWalkParameters,
                trainParameters,
                new Concurrency(4),
                NO_RANDOM_SEED
            );

            var timedAlgorithmResult = facade.node2Vec(
                node2VecGraph,
                parameters,
                jobIdMock,
                true
            ).join();

            assertThat(timedAlgorithmResult.computeMillis()).isNotNegative();

            var embeddings = timedAlgorithmResult.result().embeddings();

            assertThat(embeddings.toArray())
                .hasSize((int) node2VecGraph.nodeCount())
                .extracting(FloatVector::data)
                .as("All embeddings should have the specified `embeddingDimension`.")
                .allSatisfy(data -> assertThat(data).hasSize(embeddingDimension));
        }
    }
}
