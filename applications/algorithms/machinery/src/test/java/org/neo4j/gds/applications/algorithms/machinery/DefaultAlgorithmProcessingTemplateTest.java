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
package org.neo4j.gds.applications.algorithms.machinery;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.metrics.ExecutionMetric;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pinky promise: next time we add or change functionality here,
 * we break this class apart so that we can test it more sensibly. Smaller chunks, more readable class,
 * and tests that are not pages long.
 */
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
            RequestScopedDependencies.builder().with(databaseId).with(user).build()
        );

        var graphName = GraphName.parse("some graph");
        var configuration = new ExampleConfiguration();
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        when(graphStoreCatalogService.getGraphResources(
            graphName,
            configuration,
            Optional.empty(),
            user,
            databaseId
        )).thenReturn(new GraphResources(graphStore, graph, ResultStore.EMPTY));

        // We need it to not be null :shrug:
        when(algorithmMetricsService.create("Duckstra")).thenReturn(mock(ExecutionMetric.class));

        //noinspection unchecked
        AlgorithmComputation<ExampleResult> computation = mock(AlgorithmComputation.class);
        var pathFindingResult = mock(ExampleResult.class);
        when(computation.compute(graph)).thenReturn(pathFindingResult);

        var resultBuilder = new ResultBuilder<ExampleConfiguration, ExampleResult, Stream<String>, Void>() {
            @Override
            public Stream<String> build(
                Graph graph,
                GraphStore graphStore,
                ExampleConfiguration configuration,
                Optional<ExampleResult> pathFindingResult,
                AlgorithmProcessingTimings timings,
                Optional<Void> metadata
            ) {
                // we skip timings when no side effects requested
                assertThat(timings.mutateOrWriteMillis).isEqualTo(-1);

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
     * From Dijkstra, the contract is that the mutate and write side effects will set relationshipsWritten.
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
            RequestScopedDependencies.builder().with(databaseId).with(user).build()
        );

        var graphName = GraphName.parse("some graph");
        var configuration = new ExampleConfiguration();
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        when(graphStoreCatalogService.getGraphResources(
            graphName,
            configuration,
            Optional.empty(),
            user,
            databaseId
        )).thenReturn(new GraphResources(graphStore, graph, ResultStore.EMPTY));

        var pathFindingResult = mock(ExampleResult.class);
        var resultBuilder = new ResultBuilder<ExampleConfiguration, ExampleResult, String, Long>() {
            @Override
            public String build(
                Graph actualGraph,
                GraphStore actualGraphStore,
                ExampleConfiguration configuration,
                Optional<ExampleResult> actualResult,
                AlgorithmProcessingTimings timings,
                Optional<Long> metadata
            ) {
                assertThat(actualGraph).isEqualTo(graph);
                assertThat(actualGraphStore).isEqualTo(graphStore);
                assertThat(actualResult).hasValue(pathFindingResult);

                assertThat(metadata.orElseThrow()).isEqualTo(42L);

                return "all assertions green!";
            }
        };

        // We need it to not be null :shrug:
        when(algorithmMetricsService.create("m || w")).thenReturn(mock(ExecutionMetric.class));

        //noinspection unchecked
        AlgorithmComputation<ExampleResult> computation = mock(AlgorithmComputation.class);
        when(computation.compute(graph)).thenReturn(pathFindingResult);

        var mutateOrWriteStep = new MutateOrWriteStep<ExampleResult, Long>() {
            @Override
            public Long execute(
                Graph graph,
                GraphStore graphStore,
                ResultStore resultStore,
                ExampleResult resultFromAlgorithm
            ) {
                return 42L;
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

        assertThat(relationshipsWritten).isEqualTo("all assertions green!");
    }

    @Test
    void shouldDoTimingsAndCounts() {
        var algorithmProcessingTemplate = new DefaultAlgorithmProcessingTemplate(
            null,
            null,
            null,
            MemoryGuard.DISABLED,
            null
        ) {
            @Override
            <CONFIGURATION extends AlgoBaseConfig> GraphResources graphLoadAndValidationWithTiming(
                AlgorithmProcessingTimingsBuilder timingsBuilder,
                GraphName graphName,
                CONFIGURATION configuration
            ) {
                timingsBuilder.withPreProcessingMillis(23);
                return new GraphResources(null, mock(Graph.class), null);
            }

            @Override
            <RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeWithTiming(
                AlgorithmProcessingTimingsBuilder timingsBuilder,
                String humanReadableAlgorithmName,
                AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
                Graph graph
            ) {
                timingsBuilder.withComputeMillis(117);
                return null;
            }

            @Override
            <RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> MUTATE_OR_WRITE_METADATA mutateOrWriteWithTiming(
                Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA>> mutateOrWriteStep,
                AlgorithmProcessingTimingsBuilder timingsBuilder,
                Graph graph,
                GraphStore graphStore,
                ResultStore resultStore,
                RESULT_FROM_ALGORITHM resultFromAlgorithm
            ) {
                timingsBuilder.withMutateOrWriteMillis(87);
                return mutateOrWriteStep.orElseThrow().execute(graph, graphStore, resultStore, resultFromAlgorithm);
            }
        };

        var resultBuilder = new ResultBuilder<ExampleConfiguration, Void, Map<String, Long>, Long>() {
            @Override
            public Map<String, Long> build(
                Graph graph,
                GraphStore graphStore,
                ExampleConfiguration configuration,
                Optional<Void> unused,
                AlgorithmProcessingTimings timings,
                Optional<Long> metadata
            ) {
                return Map.of(
                    "preProcessingMillis", timings.preProcessingMillis,
                    "computeMillis", timings.computeMillis,
                    "postProcessingMillis", timings.mutateOrWriteMillis,
                    "relationshipsWritten", metadata.orElseThrow()
                );
            }
        };

        var resultMap = algorithmProcessingTemplate.processAlgorithm(
            null,
            null,
            null,
            null,
            null,
            Optional.of((graph, graphStore, resultStore, unused) -> 6573L),
            resultBuilder
        );

        assertThat(resultMap)
            .containsOnly(
                entry("preProcessingMillis", 23L),
                entry("computeMillis", 117L),
                entry("postProcessingMillis", 87L),
                entry("relationshipsWritten", 6573L)
            );
    }
}
