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
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.ConductanceAlgorithmFactory;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.k1coloring.K1ColoringAlgorithmFactory;
import org.neo4j.gds.k1coloring.K1ColoringBaseConfig;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.kcore.KCoreDecompositionAlgorithmFactory;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kmeans.KmeansAlgorithmFactory;
import org.neo4j.gds.kmeans.KmeansBaseConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationFactory;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.leiden.LeidenAlgorithmFactory;
import org.neo4j.gds.leiden.LeidenBaseConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.louvain.LouvainAlgorithmFactory;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityCalculatorFactory;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.scc.SccAlgorithmFactory;
import org.neo4j.gds.scc.SccBaseConfig;
import org.neo4j.gds.triangle.IntersectingTriangleCountFactory;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientFactory;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
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

    AlgorithmComputationResult<DisjointSetStruct> wcc(
        String graphName,
        WccBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new WccAlgorithmFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<TriangleCountResult> triangleCount(
        String graphName,
        TriangleCountBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            Optional.empty(),
            new IntersectingTriangleCountFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<KCoreDecompositionResult> kCore(
        String graphName,
        KCoreDecompositionBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            Optional.empty(),
            new KCoreDecompositionAlgorithmFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<LouvainResult> louvain(
        String graphName,
        LouvainBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new LouvainAlgorithmFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<LeidenResult> leiden(
        String graphName,
        LeidenBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new LeidenAlgorithmFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<LabelPropagationResult> labelPropagation(
        String graphName,
        LabelPropagationBaseConfig configuration,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            configuration,
            configuration.relationshipWeightProperty(),
            new LabelPropagationFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<HugeLongArray> scc(
        String graphName,
        SccBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            Optional.empty(),
            new SccAlgorithmFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<ModularityResult> modularity(
        String graphName,
        ModularityBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new ModularityCalculatorFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<KmeansResult> kmeans(
        String graphName,
        KmeansBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            Optional.empty(),
            new KmeansAlgorithmFactory<>(),
            user,
            databaseId
        );
    }

    public AlgorithmComputationResult<LocalClusteringCoefficientResult> localClusteringCoefficient(
        String graphName,
        LocalClusteringCoefficientBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            Optional.empty(),
            new LocalClusteringCoefficientFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<K1ColoringResult> k1Coloring(
        String graphName,
        K1ColoringBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            Optional.empty(),
            new K1ColoringAlgorithmFactory<>(),
            user,
            databaseId
        );
    }

    AlgorithmComputationResult<ConductanceResult> conductance(
        String graphName,
        ConductanceBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new ConductanceAlgorithmFactory<>(),
            user,
            databaseId
        );
    }

    private <A extends Algorithm<R>, R, C extends AlgoBaseConfig> AlgorithmComputationResult<R> run(
        String graphName,
        C config,
        Optional<String> relationshipProperty,
        GraphAlgorithmFactory<A, C> algorithmFactory,
        User user,
        DatabaseId databaseId
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
}
