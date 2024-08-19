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
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.PostLoadValidationHook;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class DefaultAlgorithmProcessingTemplate implements AlgorithmProcessingTemplate {
    private final Log log;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final MemoryGuard memoryGuard;
    private final RequestScopedDependencies requestScopedDependencies;

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
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA>> mutateOrWriteStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        // as we progress through the steps we gather timings
        var timingsBuilder = new AlgorithmProcessingTimingsBuilder();

        var graphResources = graphLoadAndValidationWithTiming(
            timingsBuilder,
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks
        );

        var result = runComputation(
            configuration,
            graphResources.graph(),
            graphResources.graphStore(),
            label,
            estimationFactory,
            algorithmComputation,
            timingsBuilder
        );

        var metadata = mutateOrWriteWithTiming(
            mutateOrWriteStep,
            timingsBuilder,
            graphResources.graph(),
            graphResources.graphStore(),
            graphResources.resultStore(),
            result,
            configuration.jobId()
        );

        // inject dependencies to render results
        return resultBuilder.build(
            graphResources.graph(),
            graphResources.graphStore(),
            configuration,
            result,
            timingsBuilder.build(),
            metadata
        );
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM> Stream<RESULT_TO_CALLER> processAlgorithmForStream(
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        StreamResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {

        var timingsBuilder = new AlgorithmProcessingTimingsBuilder();
        var graphResources = graphLoadAndValidationWithTiming(
            timingsBuilder,
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks
        );

        var result = runComputation(
            configuration,
            graphResources.graph(),
            graphResources.graphStore(),
            label,
            estimationFactory,
            algorithmComputation,
            timingsBuilder
        );

        // inject dependencies to render results
        return resultBuilder.build(
            graphResources.graph(),
            graphResources.graphStore(),
            configuration,
            result
        );
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM> RESULT_TO_CALLER processAlgorithmForStats(
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        StatsResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {
        var timingsBuilder = new AlgorithmProcessingTimingsBuilder();
        var graphResources = graphLoadAndValidationWithTiming(
            timingsBuilder,
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks
        );

        var result = runComputation(
            configuration,
            graphResources.graph(),
            graphResources.graphStore(),
            label,
            estimationFactory,
            algorithmComputation,
            timingsBuilder
        );

        // inject dependencies to render results
        return resultBuilder.build(
            graphResources.graph(),
            configuration,
            result,
            timingsBuilder.build()
        );
    }

    private <RESULT_FROM_ALGORITHM,CONFIGURATION extends  AlgoBaseConfig> Optional<RESULT_FROM_ALGORITHM> runComputation(
        CONFIGURATION configuration,
        Graph graph,
        GraphStore graphStore,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        AlgorithmProcessingTimingsBuilder timingsBuilder
    ){

        if (graph.isEmpty()){
            return Optional.empty();
        }

        memoryGuard.assertAlgorithmCanRun(label, configuration, graph, estimationFactory);
        // do the actual computation
        var result = computeWithTiming(
            timingsBuilder,
            label,
            algorithmComputation,
            graph,
            graphStore
        );

        return Optional.ofNullable(result);
    }

    /**
     * We have a convention here for determining relationship property. Most use cases follow the convention.
     * But because at least one use case does not, there is _also_ an override.
     */
    <CONFIGURATION extends AlgoBaseConfig> GraphResources graphLoadAndValidationWithTiming(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(timingsBuilder::withPreProcessingMillis)) {
            var relationshipProperty = determineRelationshipProperty(configuration, relationshipWeightOverride);

            return graphStoreCatalogService.getGraphResources(
                graphName,
                configuration,
                postGraphStoreLoadValidationHooks,
                relationshipProperty,
                requestScopedDependencies.getUser(),
                requestScopedDependencies.getDatabaseId()
            );
        }
    }

    /**
     * Use the override if supplied; otherwise interrogate if the type is RelationshipWeightConfig, and if so, use that.
     */
    private <CONFIGURATION> Optional<String> determineRelationshipProperty(
        CONFIGURATION configuration,
        Optional<String> relationshipWeightOverride
    ) {
        if (relationshipWeightOverride.isPresent()) return relationshipWeightOverride;

        if (configuration instanceof RelationshipWeightConfig)
            return ((RelationshipWeightConfig) configuration).relationshipWeightProperty();

        return Optional.empty();
    }

    <RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeWithTiming(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        LabelForProgressTracking label,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        Graph graph,
        GraphStore graphStore
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(timingsBuilder::withComputeMillis)) {
            return computeWithMetric(label, algorithmComputation, graph, graphStore);
        }
    }

    private <RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeWithMetric(
        LabelForProgressTracking label,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        Graph graph,
        GraphStore graphStore
    ) {
        var executionMetric = algorithmMetricsService.create(label.value);

        try (executionMetric) {
            executionMetric.start();

            return algorithmComputation.compute(graph, graphStore);
        } catch (RuntimeException e) {
            log.warn("computation failed, halting metrics gathering", e);
            executionMetric.failed(e);
            throw e;
        }
    }

    /**
     * @return null if we are not in mutate or write mode; appropriate metadata otherwise
     */
    <RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> Optional<MUTATE_OR_WRITE_METADATA> mutateOrWriteWithTiming(
        Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA>> mutateOrWriteStep,
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        Optional<RESULT_FROM_ALGORITHM> result,
        JobId jobId
    ) {
        if (mutateOrWriteStep.isEmpty() || result.isEmpty()) return Optional.empty();

        try (ProgressTimer ignored = ProgressTimer.start(timingsBuilder::withMutateOrWriteMillis)) {
            return Optional.ofNullable(mutateOrWriteStep.get().execute(graph, graphStore, resultStore, result.get(), jobId));
        }
    }

}
