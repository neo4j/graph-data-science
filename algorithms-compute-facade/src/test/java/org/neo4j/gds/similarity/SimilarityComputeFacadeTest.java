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
package org.neo4j.gds.similarity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnParameters;
import org.neo4j.gds.similarity.filtering.NodeIdNodeFilterSpec;
import org.neo4j.gds.similarity.knn.K;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;
import org.neo4j.gds.similarity.knn.KnnParameters;
import org.neo4j.gds.similarity.knn.KnnSampler;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@GdlExtension
class SimilarityComputeFacadeTest {

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ProgressTrackerFactory progressTrackerFactoryMock;
    @Mock
    private ProgressTracker progressTrackerMock;

    @Mock
    private JobId jobIdMock;

    @Mock
    private Log logMock;

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String GDL = """
        (a:Node { prop: 1.0 })-[:REL]->(b:Node { prop: 2.0 }),
        (b)-[:REL]->(c:Node { prop: 3.0}),
        (b)-[:REL]->(d:Node { prop: 4.0}),
        (a)-[:REL]->(c),
        (a)-[:REL]->(d)
        """;

    @Inject
    private TestGraph graph;

    private SimilarityComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(progressTrackerMock);

        facade = new SimilarityComputeFacade(
            new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock),
            progressTrackerFactoryMock,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Test
    void knn() {
        var k = K.create(1, graph.nodeCount(), 0.5, 0.001);

        var knnParameters = new KnnParameters(
            new Concurrency(1),
            100,
            0.0,
            k,
            0.0,
            10,
            1000,
            KnnSampler.SamplerType.UNIFORM,
            Optional.of(19L),
            List.of(new KnnNodePropertySpec("prop"))
        );

        var future = facade.knn(
            graph,
            knnParameters,
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().nodePairsConsidered()).isGreaterThan(0);
        assertThat(results.result().neighborsOf(graph.toMappedNodeId("a"))).containsExactly(
            graph.toMappedNodeId("b")
        );
        assertThat(results.computeMillis()).isNotNegative();

    }

    @Test
    void filteredKnn() {
        var k = K.create(1, graph.nodeCount(), 0.5, 0.001);

        var knnParameters = new KnnParameters(
            new Concurrency(1),
            100,
            0.0,
            k,
            0.0,
            10,
            1000,
            KnnSampler.SamplerType.UNIFORM,
            Optional.of(19L),
            List.of(new KnnNodePropertySpec("prop"))
        );

        var filteredKnnParameters = new FilteredKnnParameters(
            knnParameters,
            new FilteringParameters(
                new NodeIdNodeFilterSpec(
                    Set.of(
                        graph.toOriginalNodeId("a")
                    )
                ),
                new NodeIdNodeFilterSpec(Set.of(
                    graph.toOriginalNodeId("c"),
                    graph.toOriginalNodeId("d")
                ))),
            true
        );

        var future = facade.filteredKnn(
            graph,
            filteredKnnParameters,
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().nodePairsConsidered()).isGreaterThan(0);
        assertThat(results.result().similarityResultStream()).containsExactly(
            new SimilarityResult(
                graph.toMappedNodeId("a"),
                graph.toMappedNodeId("c"),
                1/3.0
            )
        );
        assertThat(results.computeMillis()).isNotNegative();

    }

}
