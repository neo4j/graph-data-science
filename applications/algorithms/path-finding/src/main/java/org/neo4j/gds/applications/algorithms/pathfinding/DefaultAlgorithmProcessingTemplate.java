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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import java.util.Optional;
import java.util.function.Supplier;

public class DefaultAlgorithmProcessingTemplate implements AlgorithmProcessingTemplate {
    // global dependencies
    private final Log log;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final MemoryGuard memoryGuard;

    // request scoped parameters
    private final DatabaseId databaseId;
    private final User user;

    public DefaultAlgorithmProcessingTemplate(
        Log log,
        AlgorithmMetricsService algorithmMetricsService,
        GraphStoreCatalogService graphStoreCatalogService,
        MemoryGuard memoryGuard,
        DatabaseId databaseId,
        User user
    ) {
        this.log = log;
        this.algorithmMetricsService = algorithmMetricsService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.databaseId = databaseId;
        this.user = user;
        this.memoryGuard = memoryGuard;
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM> RESULT_TO_CALLER processAlgorithm(
        GraphName graphName,
        CONFIGURATION configuration,
        String humanReadableAlgorithmName,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM>> mutateOrWriteStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {
        // as we progress through the steps we gather some metadata
        var timingsBuilder = new AlgorithmProcessingTimingsBuilder();
        var countsBuilder = new SideEffectProcessingCountsBuilder();

        Pair<Graph, GraphStore> graphWithGraphStore = graphLoadAndValidationWithTiming(
            timingsBuilder,
            graphName,
            configuration,
            resultBuilder
        );

        var graph = graphWithGraphStore.getLeft();
        var graphStore = graphWithGraphStore.getRight();

        if (graph.isEmpty()) return resultBuilder.build(
            graph,
            graphStore,
            configuration,
            Optional.empty(),
            timingsBuilder.build(),
            countsBuilder.build()
        );

        memoryGuard.assertAlgorithmCanRun(humanReadableAlgorithmName, configuration, graph, estimationFactory);

        // do the actual computation
        var result = computeWithTiming(
            timingsBuilder,
            humanReadableAlgorithmName,
            algorithmComputation,
            resultBuilder,
            graph
        );

        // do any side effects
        mutateOrWriteWithTiming(mutateOrWriteStep, timingsBuilder, countsBuilder, graph, graphStore, result);

        // inject dependencies to render results
        return resultBuilder.build(
            graph,
            graphStore,
            configuration,
            Optional.ofNullable(result),
            timingsBuilder.build(),
            countsBuilder.build()
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
    <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM> Pair<Graph, GraphStore> graphLoadAndValidationWithTiming(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        GraphName graphName,
        CONFIGURATION configuration,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(timingsBuilder::withPreProcessingMillis)) {
            // tee up the graph we want to work on
            var relationshipProperty = extractRelationshipProperty(configuration);

            var graphWithGraphStore = graphStoreCatalogService.getGraphWithGraphStore(
                graphName,
                configuration,
                relationshipProperty,
                user,
                databaseId
            );

            // ValidationConfiguration post-load stuff would go here

            return graphWithGraphStore;
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

    <CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> RESULT_FROM_ALGORITHM computeWithTiming(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        String humanReadableAlgorithmName,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder,
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
            executionMetric.failed();
            throw e;
        }
    }

    <RESULT_FROM_ALGORITHM> void mutateOrWriteWithTiming(
        Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM>> mutateOrWriteStep,
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        SideEffectProcessingCountsBuilder countsBuilder,
        Graph graph,
        GraphStore graphStore,
        RESULT_FROM_ALGORITHM result
    ) {
        mutateOrWriteStep.ifPresent(step -> {
            try (ProgressTimer ignored = ProgressTimer.start(timingsBuilder::withPostProcessingMillis)) {
                step.execute(graph, graphStore, result, countsBuilder);
            }
        });
    }
}
