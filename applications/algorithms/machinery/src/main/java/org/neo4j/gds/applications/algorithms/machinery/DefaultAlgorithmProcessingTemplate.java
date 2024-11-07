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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.PostLoadETLHook;
import org.neo4j.gds.core.loading.PostLoadValidationHook;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class DefaultAlgorithmProcessingTemplate implements AlgorithmProcessingTemplate {
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final RequestScopedDependencies requestScopedDependencies;
    private final ComputationService computationService;

    DefaultAlgorithmProcessingTemplate(
        GraphStoreCatalogService graphStoreCatalogService,
        RequestScopedDependencies requestScopedDependencies,
        ComputationService computationService
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.requestScopedDependencies = requestScopedDependencies;
        this.computationService = computationService;
    }

    public static DefaultAlgorithmProcessingTemplate create(
        Log log,
        AlgorithmMetricsService algorithmMetricsService,
        GraphStoreCatalogService graphStoreCatalogService,
        MemoryGuard memoryGuard,
        RequestScopedDependencies requestScopedDependencies
    ) {
        var algorithmComputer = new ComputationService(log, memoryGuard, algorithmMetricsService);

        return new DefaultAlgorithmProcessingTemplate(
            graphStoreCatalogService,
            requestScopedDependencies,
            algorithmComputer
        );
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM, MUTATE_METADATA> RESULT_TO_CALLER processAlgorithmForMutate(
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        Optional<Iterable<PostLoadETLHook>> postGraphStoreLoadETLHooks,
        Label label,
        Supplier<MemoryEstimation> estimationSupplier,
        Computation<RESULT_FROM_ALGORITHM> computation,
        MutateStep<RESULT_FROM_ALGORITHM, MUTATE_METADATA> mutateStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_METADATA> resultBuilder
    ) {
        var mutateEffect = new MutateSideEffect<>(mutateStep);
        var resultRenderer = new MutateResultRenderer<>(configuration, resultBuilder);

        return processAlgorithmAndAnySideEffects(
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks,
            postGraphStoreLoadETLHooks,
            label,
            DimensionTransformer.DISABLED,
            estimationSupplier,
            computation,
            Optional.of(mutateEffect),
            resultRenderer
        );
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM> RESULT_TO_CALLER processAlgorithmForStats(
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        Optional<Iterable<PostLoadETLHook>> postGraphStoreLoadETLHooks,
        Label label,
        Supplier<MemoryEstimation> estimationSupplier,
        Computation<RESULT_FROM_ALGORITHM> computation,
        StatsResultBuilder<RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {
        var resultRenderer = new StatsResultRenderer<>(resultBuilder);

        return processAlgorithmAndAnySideEffects(
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks,
            postGraphStoreLoadETLHooks,
            label,
            DimensionTransformer.DISABLED,
            estimationSupplier,
            computation,
            Optional.empty(),
            resultRenderer
        );
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM> Stream<RESULT_TO_CALLER> processAlgorithmForStream(
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        Optional<Iterable<PostLoadETLHook>> postGraphStoreLoadETLHooks,
        Label label,
        Supplier<MemoryEstimation> estimationSupplier,
        Computation<RESULT_FROM_ALGORITHM> computation,
        StreamResultBuilder<RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {
        var resultRenderer = new StreamResultRenderer<>(resultBuilder);

        return processAlgorithmAndAnySideEffects(
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks,
            postGraphStoreLoadETLHooks,
            label,
            DimensionTransformer.DISABLED,
            estimationSupplier,
            computation,
            Optional.empty(),
            resultRenderer
        );
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM, WRITE_METADATA> RESULT_TO_CALLER processAlgorithmForWrite(
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        Optional<Iterable<PostLoadETLHook>> postGraphStoreLoadETLHooks,
        Label label,
        Supplier<MemoryEstimation> estimationSupplier,
        Computation<RESULT_FROM_ALGORITHM> computation,
        WriteStep<RESULT_FROM_ALGORITHM, WRITE_METADATA> writeStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, WRITE_METADATA> resultBuilder
    ) {
        var writeEffect = new WriteSideEffect<>(configuration.jobId(), writeStep);
        var resultRenderer = new WriteResultRenderer<>(configuration, resultBuilder);

        return processAlgorithmAndAnySideEffects(
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks,
            postGraphStoreLoadETLHooks,
            label,
            DimensionTransformer.DISABLED,
            estimationSupplier,
            computation,
            Optional.of(writeEffect),
            resultRenderer
        );
    }

    /**
     * This is the nice, reusable template method for all algorithms, that does four things:
     *
     * <ol>
     *     <li>Load data</li>
     *     <li>Compute algorithm</li>
     *     <li>Process any side effects, like mutation</li>
     *     <li>Render a result</li>
     * </ol>
     *
     * We instrument with timings, to separate cross-cutting boilerplate from business logic.
     */
    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM, SIDE_EFFECT_METADATA> RESULT_TO_CALLER processAlgorithmAndAnySideEffects(
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        Optional<Iterable<PostLoadETLHook>> postGraphStoreLoadETLHooks,
        Label label,
        DimensionTransformer dimensionTransformer,
        Supplier<MemoryEstimation> estimationSupplier,
        Computation<RESULT_FROM_ALGORITHM> computation,
        Optional<SideEffect<RESULT_FROM_ALGORITHM, SIDE_EFFECT_METADATA>> sideEffect,
        ResultRenderer<RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, SIDE_EFFECT_METADATA> resultRenderer
    ) {
        // as we progress through the steps we gather timings
        var timingsBuilder = new AlgorithmProcessingTimingsBuilder();

        var graphResources = loadAndValidateGraph(
            timingsBuilder,
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks,
            postGraphStoreLoadETLHooks
        );

        var result = runComputation(
            configuration,
            graphResources,
            label,
            estimationSupplier,
            computation,
            timingsBuilder,
            dimensionTransformer
        );

        var metadata = processSideEffect(timingsBuilder, graphResources, result, sideEffect);

        return resultRenderer.render(graphResources, result, timingsBuilder.build(), metadata);
    }

    /**
     * We have a convention here for determining relationship property. Most use cases follow the convention.
     * But because at least one use case does not, there is _also_ an override.
     */
    private <CONFIGURATION extends AlgoBaseConfig> GraphResources loadAndValidateGraph(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        Optional<Iterable<PostLoadETLHook>> postGraphStoreLoadETLHooks
    ) {
        try (var ignored = ProgressTimer.start(timingsBuilder::withPreProcessingMillis)) {
            var relationshipProperty = determineRelationshipProperty(configuration, relationshipWeightOverride);

            return graphStoreCatalogService.getGraphResources(
                graphName,
                configuration,
                postGraphStoreLoadValidationHooks,
                postGraphStoreLoadETLHooks,
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

    private <RESULT_FROM_ALGORITHM, CONFIGURATION extends AlgoBaseConfig> Optional<RESULT_FROM_ALGORITHM> runComputation(
        CONFIGURATION configuration,
        GraphResources graphResources,
        Label label,
        Supplier<MemoryEstimation> estimationSupplier,
        Computation<RESULT_FROM_ALGORITHM> computation,
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        DimensionTransformer dimensionTransformer
    ) {
        if (graphResources.graph().isEmpty()) return Optional.empty();

        try (var ignored = ProgressTimer.start(timingsBuilder::withComputeMillis)) {
            var result = computationService.computeAlgorithm(
                configuration,
                graphResources,
                label,
                estimationSupplier,
                computation,
                dimensionTransformer
            );

            return Optional.ofNullable(result);
        }
    }

    private <RESULT_FROM_ALGORITHM, METADATA> Optional<METADATA> processSideEffect(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        GraphResources graphResources,
        Optional<RESULT_FROM_ALGORITHM> result,
        Optional<SideEffect<RESULT_FROM_ALGORITHM, METADATA>> sideEffect
    ) {
        if (sideEffect.isEmpty()) return Optional.empty();

        try (var ignored = ProgressTimer.start(timingsBuilder::withSideEffectMillis)) { // rename
            return sideEffect.get().process(graphResources, result);
        }
    }
}
