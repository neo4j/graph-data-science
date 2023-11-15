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
import org.neo4j.gds.algorithms.RequestScopedDependencies;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class AlgorithmRunner {

    private final GraphStoreCatalogService graphStoreCatalogService;
    private final AlgorithmMemoryValidationService memoryUsageValidator;
    private final TaskRegistryFactory taskRegistryFactory;
    private final UserLogRegistryFactory userLogRegistryFactory;
    private final Log log;

    public AlgorithmRunner(
        GraphStoreCatalogService graphStoreCatalogService,
        AlgorithmMemoryValidationService memoryUsageValidator,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        Log log
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.memoryUsageValidator = memoryUsageValidator;
        this.taskRegistryFactory = taskRegistryFactory;
        this.userLogRegistryFactory = userLogRegistryFactory;
        this.log = log;
    }

    /**
     * @deprecated Strangle this one, use the other one
     */
    @Deprecated
    public <A extends Algorithm<R>, R, C extends AlgoBaseConfig> AlgorithmComputationResult<R> run(
        String graphName,
        C config,
        Optional<String> relationshipProperty,
        GraphAlgorithmFactory<A, C> algorithmFactory,
        User user,
        DatabaseId databaseId,
        TerminationFlag terminationFlag
    ) {
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(databaseId)
            .with(terminationFlag)
            .with(user)
            .build();

        return run(
            requestScopedDependencies,
            graphName,
            config,
            relationshipProperty,
            algorithmFactory
        );
    }

    public <A extends Algorithm<R>, R, C extends AlgoBaseConfig> AlgorithmComputationResult<R> run(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        C config,
        Optional<String> relationshipProperty,
        GraphAlgorithmFactory<A, C> algorithmFactory
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
            taskRegistryFactory,
            userLogRegistryFactory
        );

        // this really belongs in the factory build thing
        algorithm.setTerminationFlag(requestScopedDependencies.getTerminationFlag());

        // run the algorithm
        try {
            var algorithmResult = algorithm.compute();

            return AlgorithmComputationResult.of(algorithmResult, graph, graphStore);
        } catch (Exception e) {
            log.warn("Computation failed", e);
            algorithm.getProgressTracker().endSubTaskWithFailure();
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
