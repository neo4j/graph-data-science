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
import org.neo4j.gds.algorithms.AlgorithmMemoryEstimation;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.algorithms.ComputationResult;
import org.neo4j.gds.kcore.KCoreDecompositionAlgorithmFactory;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.wcc.WccAlgorithmFactory;
import org.neo4j.gds.wcc.WccBaseConfig;

import java.util.Optional;

public class CommunityAlgorithmsBusinessFacade {
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final AlgorithmMemoryValidationService memoryUsageValidator;

    public CommunityAlgorithmsBusinessFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        AlgorithmMemoryValidationService memoryUsageValidator
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.memoryUsageValidator = memoryUsageValidator;
    }

    public ComputationResult<WccBaseConfig, DisjointSetStruct> wcc(
        String graphName,
        WccBaseConfig config,
        User user,
        DatabaseId databaseId,
        ProgressTracker progressTracker
    ) {
        return run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new WccAlgorithmFactory<>(),
            user,
            databaseId,
            progressTracker
        );
    }

    public ComputationResult<KCoreDecompositionBaseConfig, KCoreDecompositionResult> kCore(
        String graphName,
        KCoreDecompositionBaseConfig config,
        User user,
        DatabaseId databaseId,
        ProgressTracker progressTracker
    ) {
        return run(
            graphName,
            config,
            Optional.empty(),
            new KCoreDecompositionAlgorithmFactory<>(),
            user,
            databaseId,
            progressTracker
        );
    }

    private <A extends Algorithm<R>, C extends AlgoBaseConfig, R> ComputationResult<C, R> run(
        String graphName,
        C config,
        Optional<String> relationshipProperty,
        GraphAlgorithmFactory<A, C> algorithmFactory,
        User user,
        DatabaseId databaseId,
        ProgressTracker progressTracker
    ) {
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
            return ComputationResult.withoutAlgorithmResult(graph, config, graphStore);
        }

        // create and run the algorithm
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
        var algorithm = algorithmFactory.build(graph, config, progressTracker);
        var algorithmResult = algorithm.compute();

        return ComputationResult.of(algorithmResult, graph, config, graphStore);
    }
}
