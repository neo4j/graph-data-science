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
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.fastrp.FastRPParameters;
import org.neo4j.gds.embeddings.hashgnn.BinarizeParameters;
import org.neo4j.gds.embeddings.hashgnn.HashGNNParameters;
import org.neo4j.gds.embeddings.node2vec.EmbeddingInitializer;
import org.neo4j.gds.embeddings.node2vec.Node2VecParameters;
import org.neo4j.gds.embeddings.node2vec.SamplingWalkParameters;
import org.neo4j.gds.embeddings.node2vec.TrainParameters;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.ml.core.tensor.FloatVector;
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
            new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock),
            progressTrackerFactoryMock,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Nested
    @GdlExtension
    class FastRPTest {

        @GdlGraph(graphNamePrefix = "fastRp")
        private static final String FAST_RP =
            "CREATE" +
                "  (a:Node1 {f: [0.4, 1.3, 1.4]})" +
                ", (b:Node1 {f: [2.1, 0.5, 1.8]})" +
                ", (c:Node2 {f: [-0.3, 0.8, 2.8]})" +
                ", (d:Isolated {f: [2.5, 8.1, 1.3]})" +
                ", (e:Isolated {f: [0.6, 0.5, 5.2]})" +
                ", (a)-[:REL {weight: 2.0}]->(b)" +
                ", (b)-[:REL {weight: 1.0}]->(a)" +
                ", (a)-[:REL {weight: 1.0}]->(c)" +
                ", (c)-[:REL {weight: 1.0}]->(a)" +
                ", (b)-[:REL {weight: 1.0}]->(c)" +
                ", (c)-[:REL {weight: 1.0}]->(b)";


        @Inject
        private Graph fastRpGraph;

        private static final int DEFAULT_EMBEDDING_DIMENSION = 128;

        @Test
        void fastRP() {

            var parameters = new FastRPParameters(
                List.of("f"),
                List.of(1.0D),
                DEFAULT_EMBEDDING_DIMENSION,
                0.0,
                Optional.empty(),
                0.0F,
                0,
                new Concurrency(4),
                Optional.of(42L)
            );

            var timedAlgorithmResult = facade.fastRP(fastRpGraph, parameters, jobIdMock, true).join();

            assertThat(timedAlgorithmResult.computeMillis()).isNotNegative();

            assertThat(timedAlgorithmResult.result().embeddings().size()).isEqualTo(fastRpGraph.nodeCount());
        }
    }

    @Nested
    @GdlExtension
    class HashGnnTest {

        @GdlGraph(graphNamePrefix = "hashGnn")
        private static final String HASH_GNN =
            "CREATE" +
                "  (a:N {f1: 1.1, f2: [1.3, 2.0]})" +
                ", (b:N {f1: 1.5, f2: [-3.1, 1.6]})" +
                ", (c:N {f1: -0.6, f2: [0.0, -1.0]})" +
                ", (b)-[:R]->(a)" +
                ", (b)-[:R]->(c)";

        @Inject
        private Graph hashGnnGraph;

        @Test
        void hashGnn() {
            int embeddingDensity = 100;
            int binarizationDimension = 8;

            var parameters = new HashGNNParameters(
                new Concurrency(4),
                1,
                embeddingDensity,
                0.0,
                List.of("f1", "f2"),
                false,
                Optional.empty(),
                Optional.of(new BinarizeParameters(binarizationDimension, 0.0d)),
                Optional.empty(),
                Optional.of(42L)
            );

            var timedAlgorithmResult = facade.hashGnn(hashGnnGraph, parameters, List.of("R"), jobIdMock, true).join();

            assertThat(timedAlgorithmResult.computeMillis()).isNotNegative();
            assertThat(timedAlgorithmResult.result().embeddings().nodeCount()).isEqualTo(hashGnnGraph.nodeCount());
        }
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
