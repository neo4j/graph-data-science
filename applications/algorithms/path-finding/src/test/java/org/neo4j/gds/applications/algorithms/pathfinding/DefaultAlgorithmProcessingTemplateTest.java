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
package org.neo4j.gds.applications.algorithms.pathfinding;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.metrics.ExecutionMetric;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultAlgorithmProcessingTemplateTest {
    @Test
    void shouldProcessStreamAlgorithm() {
        var databaseId = DatabaseId.of("some database");
        var user = new User("some user", false);
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var algorithmMetricsService = mock(AlgorithmMetricsService.class);
        var algorithmProcessingTemplate = new DefaultAlgorithmProcessingTemplate(
            null,
            algorithmMetricsService,
            graphStoreCatalogService,
            MemoryGuard.DISABLED,
            databaseId,
            user
        );

        var graphName = GraphName.parse("some graph");
        var configuration = new ExampleConfiguration();
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        when(graphStoreCatalogService.getGraphWithGraphStore(
            graphName,
            configuration,
            Optional.empty(),
            user,
            databaseId
        )).thenReturn(Pair.of(graph, graphStore));

        // We need it to not be null :shrug:
        when(algorithmMetricsService.create("Duckstra")).thenReturn(mock(ExecutionMetric.class));

        //noinspection unchecked
        AlgorithmComputation<PathFindingResult> computation = mock(AlgorithmComputation.class);
        var pathFindingResult = mock(PathFindingResult.class);
        when(computation.compute(graph)).thenReturn(pathFindingResult);

        var resultBuilder = new ResultBuilder<PathFindingResult, Stream<String>>() {
            @Override
            public Stream<String> build(
                Graph graph,
                GraphStore graphStore,
                Optional<PathFindingResult> pathFindingResult,
                AlgorithmProcessingTimings timings
            ) {
                // we skip timings when no side effects requested
                assertThat(timings.postProcessingMillis).isEqualTo(-1);

                return Stream.of(
                    "Huey",
                    "Dewey",
                    "Louie"
                );
            }
        };

        var resultStream = algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            "Duckstra",
            null,
            computation,
            Optional.empty(),
            resultBuilder
        );

        assertThat(resultStream).containsExactly(
            "Huey",
            "Dewey",
            "Louie"
        );
    }

    /**
     * From pathfinding, the contract is that the mutate and write side effects will set relationshipsWritten.
     * So that's the example we go with.
     */
    @Test
    void shouldProcessMutateOrWriteAlgorithm() {
        var databaseId = DatabaseId.of("some database");
        var user = new User("some user", false);
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var algorithmMetricsService = mock(AlgorithmMetricsService.class);
        var algorithmProcessingTemplate = new DefaultAlgorithmProcessingTemplate(
            null,
            algorithmMetricsService,
            graphStoreCatalogService,
            MemoryGuard.DISABLED,
            databaseId,
            user
        );

        var graphName = GraphName.parse("some graph");
        var configuration = new ExampleConfiguration();
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        when(graphStoreCatalogService.getGraphWithGraphStore(
            graphName,
            configuration,
            Optional.empty(),
            user,
            databaseId
        )).thenReturn(Pair.of(graph, graphStore));

        var pathFindingResult = mock(PathFindingResult.class);
        var resultBuilder = new ResultBuilder<PathFindingResult, Long>() {
            @Override
            public Long build(
                Graph actualGraph,
                GraphStore actualGraphStore,
                Optional<PathFindingResult> actualResult,
                AlgorithmProcessingTimings timings
            ) {
                assertThat(actualGraph).isEqualTo(graph);
                assertThat(actualGraphStore).isEqualTo(graphStore);
                assertThat(actualResult).hasValue(pathFindingResult);

                return relationshipsWritten;
            }
        };

        // We need it to not be null :shrug:
        when(algorithmMetricsService.create("m || w")).thenReturn(mock(ExecutionMetric.class));

        //noinspection unchecked
        AlgorithmComputation<PathFindingResult> computation = mock(AlgorithmComputation.class);
        when(computation.compute(graph)).thenReturn(pathFindingResult);

        var mutateOrWriteStep = new MutateOrWriteStep<PathFindingResult>() {
            @Override
            public <RESULT_TO_CALLER> void execute(
                Graph graph,
                GraphStore graphStore,
                PathFindingResult resultFromAlgorithm,
                ResultBuilder<PathFindingResult, RESULT_TO_CALLER> resultBuilder
            ) {
                resultBuilder.withRelationshipsWritten(42);
            }
        };

        var relationshipsWritten = algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            "m || w",
            null,
            computation,
            Optional.of(mutateOrWriteStep),
            resultBuilder
        );

        assertThat(relationshipsWritten).isEqualTo(42L);
    }

    @Test
    void shouldDoTimings() {
        var algorithmProcessingTemplate = new DefaultAlgorithmProcessingTemplate(
            null,
            null,
            null,
            MemoryGuard.DISABLED,
            null,
            null
        ) {
            @Override
            <CONFIGURATION extends AlgoBaseConfig & RelationshipWeightConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM> Pair<Graph, GraphStore> graphLoadAndValidationWithTiming(
                AlgorithmProcessingTimingsBuilder timingsBuilder,
                GraphName graphName,
                CONFIGURATION configuration,
                ResultBuilder<RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
            ) {
                timingsBuilder.withPreProcessingMillis(23);
                return Pair.of(mock(Graph.class), null);
            }

            @Override
            <RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> RESULT_FROM_ALGORITHM computeWithTiming(
                AlgorithmProcessingTimingsBuilder timingsBuilder,
                String humanReadableAlgorithmName,
                AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
                ResultBuilder<RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder,
                Graph graph
            ) {
                timingsBuilder.withComputeMillis(117);
                return null;
            }

            @Override
            <RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> void mutateOrWriteWithTiming(
                Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM>> mutateOrWriteStep,
                AlgorithmProcessingTimingsBuilder timingsBuilder,
                Graph graph,
                GraphStore graphStore,
                RESULT_FROM_ALGORITHM resultFromAlgorithm,
                ResultBuilder<RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
            ) {
                timingsBuilder.withPostProcessingMillis(87);
            }
        };

        var resultBuilder = new ResultBuilder<Void, Map<String, Long>>() {
            @Override
            public Map<String, Long> build(
                Graph graph,
                GraphStore graphStore,
                Optional<Void> unused,
                AlgorithmProcessingTimings timings
            ) {
                return Map.of(
                    "preProcessingMillis", timings.preProcessingMillis,
                    "computeMillis", timings.computeMillis,
                    "postProcessingMillis", timings.postProcessingMillis
                );
            }
        };

        var resultMap = algorithmProcessingTemplate.processAlgorithm(
            null,
            null,
            null,
            null,
            null,
            Optional.of(new MutateOrWriteStep<>() {
                @Override
                public <RESULT_TO_CALLER> void execute(
                    Graph graph,
                    GraphStore graphStore,
                    Void unused,
                    ResultBuilder<Void, RESULT_TO_CALLER> resultBuilder
                ) {
                    // do nothing, we are just catching timings
                }
            }),
            resultBuilder
        );

        assertThat(resultMap)
            .containsOnly(
                entry("preProcessingMillis", 23L),
                entry("computeMillis", 117L),
                entry("postProcessingMillis", 87L)
            );
    }
}
