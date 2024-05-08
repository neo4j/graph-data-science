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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import java.util.Optional;
import java.util.function.Supplier;

public class DefaultAlgorithmProcessingTemplate implements AlgorithmProcessingTemplate {
    // global dependencies
    private final Log log;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final MemoryGuard memoryGuard;
    private final RequestScopedDependencies requestScopedDependencies;

    // request scoped parameters

    public DefaultAlgorithmProcessingTemplate(
        Log log,
        AlgorithmMetricsService algorithmMetricsService,
        GraphStoreCatalogService graphStoreCatalogService,
        MemoryGuard memoryGuard,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.log = log;
        this.algorithmMetricsService = algorithmMetricsService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.memoryGuard = memoryGuard;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> RESULT_TO_CALLER processAlgorithm(
        GraphName graphName,
        CONFIGURATION configuration,
        String humanReadableAlgorithmName,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA>> mutateOrWriteStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        // as we progress through the steps we gather some metadata
        var timingsBuilder = new AlgorithmProcessingTimingsBuilder();

        var graphResources = graphLoadAndValidationWithTiming(
            timingsBuilder,
            graphName,
            configuration
        );

        var graph = graphResources.graph();
        var graphStore = graphResources.graphStore();
        var resultStore = graphResources.resultStore();

        if (graph.isEmpty()) return resultBuilder.build(
            graph,
            graphStore,
            configuration,
            Optional.empty(),
            timingsBuilder.build(),
            Optional.empty()
        );

        memoryGuard.assertAlgorithmCanRun(humanReadableAlgorithmName, configuration, graph, estimationFactory);

        // do the actual computation
        var result = computeWithTiming(
            timingsBuilder,
            humanReadableAlgorithmName,
            algorithmComputation,
            graph
        );

        // do any side effects
        MUTATE_OR_WRITE_METADATA metadata = mutateOrWriteWithTiming(
            mutateOrWriteStep,
            timingsBuilder,
            graph,
            graphStore,
            resultStore,
            result,
            configuration.jobId()
        );

        // inject dependencies to render results
        return resultBuilder.build(
            graph,
            graphStore,
            configuration,
            Optional.ofNullable(result),
            timingsBuilder.build(),
            Optional.ofNullable(metadata)
        );
    }

    /**
     * To fully generalise this out from pathfinding, there are two issues to solve here:
     *
     * <ul>
     *     <li>Having to have configurations inherit from both AlgoBaseConfig and RelationshipWeightConfig is not good.
     *     The stipulation for RelationshipWeightConfig could be solved with a conditional, or by lifting relationship weights up as a first class thing.
     *     (We can have a longer talk about configurations inheritance another time)</li>
     *     <li>ValidationConfiguration are a thing that is not used in path finding and so it is left out for now.
     *     Generally though there are hooks that are needed for validation: before loading, after loading, ...
     *     <p>
     *     We can add that when needed as more instrumentation.</li>
     * </ul>
     */
    <CONFIGURATION extends AlgoBaseConfig> GraphResources graphLoadAndValidationWithTiming(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        GraphName graphName,
        CONFIGURATION configuration
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(timingsBuilder::withPreProcessingMillis)) {
            // tee up the graph we want to work on
            var relationshipProperty = extractRelationshipProperty(configuration);

            var graphResources = graphStoreCatalogService.getGraphResources(
                graphName,
                configuration,
                relationshipProperty,
                requestScopedDependencies.getUser(),
                requestScopedDependencies.getDatabaseId()
            );

            // ValidationConfiguration post-load stuff would go here

            return graphResources;
        }
    }

    /**
     * Not the prettiest. Better to pass an Optional for this flag? Debatable. This is quick tho.
     */
    private static <CONFIGURATION> Optional<String> extractRelationshipProperty(CONFIGURATION configuration) {
        if (configuration instanceof RelationshipWeightConfig)
            return ((RelationshipWeightConfig) configuration).relationshipWeightProperty();

        return Optional.empty();
    }

    <RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeWithTiming(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        String humanReadableAlgorithmName,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        Graph graph
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(timingsBuilder::withComputeMillis)) {
            return computeWithMetric(humanReadableAlgorithmName, algorithmComputation, graph);
        }
    }

    private <RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeWithMetric(
        String humanReadableAlgorithmName,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        Graph graph
    ) {
        var executionMetric = algorithmMetricsService.create(humanReadableAlgorithmName);

        try (executionMetric) {
            executionMetric.start();

            return algorithmComputation.compute(graph);
            } catch (RuntimeException e) {
            log.warn("computation failed, halting metrics gathering", e);
            executionMetric.failed(e);
            throw e;
        }
    }

    /**
     * @return null if we are not in mutate or write mode; appropriate metadata otherwise
     */
    <RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> MUTATE_OR_WRITE_METADATA mutateOrWriteWithTiming(
        Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA>> mutateOrWriteStep,
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        RESULT_FROM_ALGORITHM result,
        JobId jobId
    ) {
        if (mutateOrWriteStep.isEmpty()) return null;

        try (ProgressTimer ignored = ProgressTimer.start(timingsBuilder::withMutateOrWriteMillis)) {
            return mutateOrWriteStep.get().execute(graph, graphStore, resultStore, result, jobId);
        }
    }
}
