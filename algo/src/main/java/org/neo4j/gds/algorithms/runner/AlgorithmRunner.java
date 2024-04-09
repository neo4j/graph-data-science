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
package org.neo4j.gds.algorithms.runner;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.PreconditionsProvider;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.AlgorithmMemoryEstimation;
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.algorithms.validation.AfterLoadValidation;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class AlgorithmRunner {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final AlgorithmMemoryValidationService memoryUsageValidator;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final RequestScopedDependencies requestScopedDependencies;

    public AlgorithmRunner(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        AlgorithmMetricsService algorithmMetricsService,
        AlgorithmMemoryValidationService memoryUsageValidator,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.memoryUsageValidator = memoryUsageValidator;
        this.algorithmMetricsService = algorithmMetricsService;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    public <A extends Algorithm<R>, R, C extends AlgoBaseConfig> AlgorithmComputationResult<R> run(
        String graphName,
        C config,
        Optional<String> relationshipProperty,
        GraphAlgorithmFactory<A, C> algorithmFactory
    ) {
        return run(graphName, config, relationshipProperty, algorithmFactory, List.of());
    }

    public <A extends Algorithm<R>, R, C extends AlgoBaseConfig> AlgorithmComputationResult<R> run(
        String graphName,
        C config,
        Optional<String> relationshipProperty,
        GraphAlgorithmFactory<A, C> algorithmFactory,
        List<AfterLoadValidation> afterLoadValidationsList
    ) {
        // TODO: Is this the best place to check for preconditions???
        PreconditionsProvider.preconditions().check();

        // Go get the graph and graph store from the catalog
        var graphWithGraphStore = graphStoreCatalogService.getGraphWithGraphStore(
            GraphName.parse(graphName),
            config,
            relationshipProperty,
            requestScopedDependencies.getUser(),
            requestScopedDependencies.getDatabaseId()
        );

        var graph = graphWithGraphStore.getLeft();
        var graphStore = graphWithGraphStore.getRight();

        afterLoadValidationsList.forEach(afterLoadValidation -> afterLoadValidation.afterLoadValidations(graphStore));

        // No algorithm execution when the graph is empty
        if (graph.isEmpty()) {
            return AlgorithmComputationResult.withoutAlgorithmResult(graph, graphStore);
        }

        // create the algorithm
        var algorithmEstimator = new AlgorithmMemoryEstimation<>(
            GraphDimensions.of(
                graph.nodeCount(),
                graph.relationshipCount()
            ),
            algorithmFactory
        );

        memoryUsageValidator.validateAlgorithmCanRunWithTheAvailableMemory(
            config,
            algorithmEstimator::memoryEstimation,
            graphStoreCatalogService.graphStoreCount()
        );
        var algorithm = algorithmFactory.build(
            graph,
            config,
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            requestScopedDependencies.getTaskRegistryFactory(),
            requestScopedDependencies.getUserLogRegistryFactory()
        );

        // this really belongs in the factory build thing
        algorithm.setTerminationFlag(requestScopedDependencies.getTerminationFlag());

        // run the algorithm
        var algorithmResult = runAlgorithm(algorithm, algorithmFactory.taskName());

        return AlgorithmComputationResult.of(algorithmResult, graph, graphStore);
    }

    <R> R runAlgorithm(Algorithm<R> algorithm, String algorithmName) {
        var algorithmMetric = algorithmMetricsService.create(algorithmName);
        try (algorithmMetric) {
            algorithmMetric.start();
            return algorithm.compute();
        } catch (Exception e) {
            log.warn("Computation failed", e);
            algorithm.getProgressTracker().endSubTaskWithFailure();
            algorithmMetric.failed();
            throw e;
        }
    }

    public static <T> AlgorithmResultWithTiming<T> runWithTiming(Supplier<T> function) {

        var computeMilliseconds = new AtomicLong();
        T algorithmResult;
        try (var ignored = ProgressTimer.start(computeMilliseconds::set)) {
            algorithmResult = function.get();
        }

        return new AlgorithmResultWithTiming<>(algorithmResult, computeMilliseconds.get());
    }


}
