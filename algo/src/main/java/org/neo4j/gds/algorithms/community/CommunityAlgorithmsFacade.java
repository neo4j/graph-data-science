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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.PreconditionsProvider;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.AlgorithmMemoryEstimation;
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.kcore.KCoreDecompositionAlgorithmFactory;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.louvain.LouvainAlgorithmFactory;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.wcc.WccAlgorithmFactory;
import org.neo4j.gds.wcc.WccBaseConfig;

import java.util.Optional;

public class CommunityAlgorithmsFacade {
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final TaskRegistryFactory taskRegistryFactory;
    private final UserLogRegistryFactory userLogRegistryFactory;
    private final AlgorithmMemoryValidationService memoryUsageValidator;
    private final Log log;

    public CommunityAlgorithmsFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        AlgorithmMemoryValidationService memoryUsageValidator,
        Log log
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.taskRegistryFactory = taskRegistryFactory;
        this.userLogRegistryFactory = userLogRegistryFactory;
        this.memoryUsageValidator = memoryUsageValidator;
        this.log = log;
    }

    <C extends WccBaseConfig> AlgorithmComputationResult<C, DisjointSetStruct> wcc(
        String graphName,
        C config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new WccAlgorithmFactory<>(),
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory
        );
    }

    <C extends KCoreDecompositionBaseConfig> AlgorithmComputationResult<C, KCoreDecompositionResult> kCore(
        String graphName,
        C config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            Optional.empty(),
            new KCoreDecompositionAlgorithmFactory<>(),
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory
        );
    }

    <C extends LouvainBaseConfig> AlgorithmComputationResult<C, LouvainResult> louvain(
        String graphName,
        C config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new LouvainAlgorithmFactory<>(),
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory
        );
    }


    private <A extends Algorithm<R>, C extends AlgoBaseConfig, R> AlgorithmComputationResult<C, R> run(
        String graphName,
        C config,
        Optional<String> relationshipProperty,
        GraphAlgorithmFactory<A, C> algorithmFactory,
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory
    ) {
        // TODO: Is this the best place to check for preconditions???
        PreconditionsProvider.preconditions().check();

        // Go get the graph and graph store from the catalog
        var graphWithGraphStore = graphStoreCatalogService.getGraphWithGraphStore(
            GraphName.parse(graphName),
            config,
            relationshipProperty,
            user,
            databaseId
        );

        var graph = graphWithGraphStore.getLeft();
        var graphStore = graphWithGraphStore.getRight();

        // No algorithm execution when the graph is empty
        if (graph.isEmpty()) {
            return AlgorithmComputationResult.withoutAlgorithmResult(graph, config, graphStore);
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

        // run the algorithm
        try {
            var algorithmResult = algorithm.compute();

            return AlgorithmComputationResult.of(algorithmResult, graph, config, graphStore);
        } catch (Exception e) {
            log.warn("Computation failed", e);
            algorithm.getProgressTracker().endSubTaskWithFailure();
            throw e;
        }
    }
}
