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
import org.neo4j.gds.applications.algorithms.metadata.Algorithm;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.PostLoadValidationHook;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.metrics.ExecutionMetric;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Dijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.KNN;

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
            Optional.empty(),
            user,
            databaseId
        )).thenReturn(new GraphResources(graphStore, graph, ResultStore.EMPTY));

        // We need it to not be null :shrug:
        when(algorithmMetricsService.create(Dijkstra.labelForProgressTracking)).thenReturn(mock(ExecutionMetric.class));

        //noinspection unchecked
        AlgorithmComputation<ExampleResult> computation = mock(AlgorithmComputation.class);
        var pathFindingResult = mock(ExampleResult.class);
        when(computation.compute(graph, graphStore)).thenReturn(pathFindingResult);

        var resultBuilder = new StreamResultBuilder<ExampleResult, String>() {
            @Override
            public Stream<String> build(
                Graph graph,
                GraphStore graphStore,
                Optional<ExampleResult> pathFindingResult
            ) {

                return Stream.of(
                    "Huey",
                    "Dewey",
                    "Louie"
                );
            }
        };

        var resultStream = algorithmProcessingTemplate.processAlgorithmForStream(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            Dijkstra,
            null,
            computation,
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
    void shouldProcessWriteAlgorithm() {
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
            Optional.empty(),
            user,
            databaseId
        )).thenReturn(new GraphResources(graphStore, graph, ResultStore.EMPTY));

        var pathFindingResult = mock(ExampleResult.class);
        var resultBuilder = new ResultBuilder<ExampleConfiguration, ExampleResult, String, Long>() {
            @Override
            public String build(
                Graph actualGraph,
                ExampleConfiguration configuration,
                Optional<ExampleResult> actualResult,
                AlgorithmProcessingTimings timings,
                Optional<Long> metadata
            ) {
                assertThat(actualGraph).isEqualTo(graph);
                assertThat(actualResult).hasValue(pathFindingResult);

                assertThat(metadata.orElseThrow()).isEqualTo(42L);

                return "all assertions green!";
            }
        };

        // We need it to not be null :shrug:
        when(algorithmMetricsService.create(KNN.labelForProgressTracking)).thenReturn(mock(ExecutionMetric.class));

        //noinspection unchecked
        AlgorithmComputation<ExampleResult> computation = mock(AlgorithmComputation.class);
        when(computation.compute(graph, graphStore)).thenReturn(pathFindingResult);

        var writeStep = new WriteStep<ExampleResult, Long>() {
            @Override
            public Long execute(
                Graph graph,
                GraphStore graphStore,
                ResultStore resultStore,
                ExampleResult resultFromAlgorithm,
                JobId jobId
            ) {
                return 42L;
            }
        };

        var relationshipsWritten = algorithmProcessingTemplate.processAlgorithmForWrite(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            KNN,
            null,
            computation,
            writeStep,
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
                Optional<String> relationshipWeightOverride,
                GraphName graphName,
                CONFIGURATION configuration,
                Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks
            ) {
                timingsBuilder.withPreProcessingMillis(23);
                return new GraphResources(null, mock(Graph.class), null);
            }

            @Override
            <RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeWithTiming(
                AlgorithmProcessingTimingsBuilder timingsBuilder,
                Algorithm algorithmMetadata,
                AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
                Graph graph,
                GraphStore graphStore
            ) {
                timingsBuilder.withComputeMillis(117);
                return null;
            }

            @Override
            <RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> Optional<MUTATE_OR_WRITE_METADATA> writeWithTiming(
                WriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> mutateOrWriteStep,
                AlgorithmProcessingTimingsBuilder timingsBuilder,
                Graph graph,
                GraphStore graphStore,
                ResultStore resultStore,
                Optional<RESULT_FROM_ALGORITHM> resultFromAlgorithm,
                JobId jobId
            ) {
                timingsBuilder.withMutateOrWriteMillis(87);
                return Optional.of(
                    mutateOrWriteStep.execute(
                        graph,
                        graphStore,
                        resultStore,
                        null,
                        jobId
                     )
                );
            }
        };

        var resultBuilder = new ResultBuilder<ExampleConfiguration, Void, Map<String, Long>, Long>() {
            @Override
            public Map<String, Long> build(
                Graph graph,
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

        var resultMap = algorithmProcessingTemplate.processAlgorithmForWrite(
            Optional.empty(),
            null,
            new ExampleConfiguration(),
            Optional.empty(),
            null,
            null,
            null,
            (graph, graphStore, resultStore, unused, jobId) -> 6573L,
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
