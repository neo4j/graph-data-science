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
package org.neo4j.gds.procedures.integration;

import org.neo4j.gds.ProcedureCallContextReturnColumns;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.algorithms.mutateservices.MutateNodePropertyService;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.similarity.MutateRelationshipService;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.algorithms.similarity.WriteRelationshipService;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithms;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsFacade;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.algorithms.ConfigurationCreator;
import org.neo4j.gds.procedures.centrality.CentralityProcedureFacade;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.embeddings.NodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.pathfinding.PathFindingProcedureFacade;
import org.neo4j.gds.procedures.similarity.SimilarityProcedureFacade;
import org.neo4j.gds.termination.TerminationFlag;

class AlgorithmProcedureFacadeProvider {
    // Global scoped dependencies
    private final Log log;

    // Request scoped parameters
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final ConfigurationCreator configurationCreator;
    private final NodeLookup nodeLookup;
    private final ProcedureCallContextReturnColumns returnColumns;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final WriteNodePropertyService writeNodePropertyService;
    private final MutateRelationshipService mutateRelationshipService;
    private final WriteRelationshipService writeRelationshipService;
    private final AlgorithmEstimator algorithmEstimator;
    private final AlgorithmRunner algorithmRunner;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;
    private final RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;
    private final UserLogRegistryFactory userLogRegistryFactory;

    AlgorithmProcedureFacadeProvider(
        Log log,
        CloseableResourceRegistry closeableResourceRegistry,
        ConfigurationCreator configurationCreator,
        NodeLookup nodeLookup,
        ProcedureCallContextReturnColumns returnColumns,
        MutateNodePropertyService mutateNodePropertyService,
        WriteNodePropertyService writeNodePropertyService,
        MutateRelationshipService mutateRelationshipService,
        WriteRelationshipService writeRelationshipService,
        AlgorithmRunner algorithmRunner,
        AlgorithmEstimator algorithmEstimator,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory
    ) {
        this.log = log;

        this.closeableResourceRegistry = closeableResourceRegistry;
        this.configurationCreator = configurationCreator;
        this.nodeLookup = nodeLookup;
        this.returnColumns = returnColumns;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.writeNodePropertyService = writeNodePropertyService;
        this.mutateRelationshipService = mutateRelationshipService;
        this.writeRelationshipService = writeRelationshipService;
        this.algorithmRunner = algorithmRunner;
        this.algorithmEstimator = algorithmEstimator;

        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
        this.relationshipStreamExporterBuilder = relationshipStreamExporterBuilder;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationFlag = terminationFlag;
        this.userLogRegistryFactory = userLogRegistryFactory;
    }

    CentralityProcedureFacade createCentralityProcedureFacade() {

        // algorithm facade
        var centralityAlgorithmsFacade = new CentralityAlgorithmsFacade(algorithmRunner);

        var estimateBusinessFacade = new CentralityAlgorithmsEstimateBusinessFacade(algorithmEstimator);
        var mutateBusinessFacade = new CentralityAlgorithmsMutateBusinessFacade(
            centralityAlgorithmsFacade,
            mutateNodePropertyService

        );
        var statsBusinessFacade = new CentralityAlgorithmsStatsBusinessFacade(centralityAlgorithmsFacade);
        var streamBusinessFacade = new CentralityAlgorithmsStreamBusinessFacade(centralityAlgorithmsFacade);
        var writeBusinessFacade = new CentralityAlgorithmsWriteBusinessFacade(
            centralityAlgorithmsFacade,
            writeNodePropertyService
        );

        // procedure facade
        return new CentralityProcedureFacade(
            configurationCreator,
            returnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );
    }

    CommunityProcedureFacade createCommunityProcedureFacade() {

        // algorithm facade
        var communityAlgorithmsFacade = new CommunityAlgorithmsFacade(algorithmRunner);

        var estimateBusinessFacade = new CommunityAlgorithmsEstimateBusinessFacade(algorithmEstimator);
        var mutateBusinessFacade = new CommunityAlgorithmsMutateBusinessFacade(
            communityAlgorithmsFacade,
            mutateNodePropertyService

        );
        var statsBusinessFacade = new CommunityAlgorithmsStatsBusinessFacade(communityAlgorithmsFacade);
        var streamBusinessFacade = new CommunityAlgorithmsStreamBusinessFacade(communityAlgorithmsFacade);
        var writeBusinessFacade = new CommunityAlgorithmsWriteBusinessFacade(
            writeNodePropertyService, communityAlgorithmsFacade
        );

        // procedure facade
        return new CommunityProcedureFacade(
            configurationCreator,
            returnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );
    }

    SimilarityProcedureFacade createSimilarityProcedureFacade() {
        // algorithms facade
        var similarityAlgorithmsFacade = new SimilarityAlgorithmsFacade(algorithmRunner);

        // mode-specific facades
        var estimateBusinessFacade = new SimilarityAlgorithmsEstimateBusinessFacade(algorithmEstimator);
        var mutateBusinessFacade = new SimilarityAlgorithmsMutateBusinessFacade(
            similarityAlgorithmsFacade,
            mutateRelationshipService
        );
        var statsBusinessFacade = new SimilarityAlgorithmsStatsBusinessFacade(similarityAlgorithmsFacade);
        var streamBusinessFacade = new SimilarityAlgorithmsStreamBusinessFacade(similarityAlgorithmsFacade);
        var writeBusinessFacade = new SimilarityAlgorithmsWriteBusinessFacade(
            similarityAlgorithmsFacade,
            writeRelationshipService
        );

        // procedure facade
        return new SimilarityProcedureFacade(
            configurationCreator,
            returnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );

    }

    PathFindingProcedureFacade createPathFindingProcedureFacade() {
        var pathFindingAlgorithms = new PathFindingAlgorithms(
            log,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory
        );

        var pathFindingAlgorithmsFacade = new PathFindingAlgorithmsFacade(
            log,
            algorithmProcessingTemplate,
            algorithmEstimationTemplate,
            relationshipStreamExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            pathFindingAlgorithms
        );

        return new PathFindingProcedureFacade(
            closeableResourceRegistry,
            configurationCreator,
            nodeLookup,
            returnColumns,
            pathFindingAlgorithmsFacade
        );
    }

    NodeEmbeddingsProcedureFacade createNodeEmbeddingsProcedureFacade() {
        // algorithms facade
        var nodeEmbeddingsAlgorithmsFacade = new NodeEmbeddingsAlgorithmsFacade(algorithmRunner);

        // mode-specific facades

        var mutateBusinessFacade = new NodeEmbeddingsAlgorithmsMutateBusinessFacade(
            nodeEmbeddingsAlgorithmsFacade,
            mutateNodePropertyService
        );

        var streamBusinessFacade = new NodeEmbeddingsAlgorithmStreamBusinessFacade(nodeEmbeddingsAlgorithmsFacade);

        var writeBusinessFacade = new NodeEmbeddingsAlgorithmsWriteBusinessFacade(
            nodeEmbeddingsAlgorithmsFacade,
            writeNodePropertyService
        );

        var estimateBusinessFacade = new NodeEmbeddingsAlgorithmsEstimateBusinessFacade(algorithmEstimator);

        // procedure facade
        return new NodeEmbeddingsProcedureFacade(
            configurationCreator,
            returnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );
    }
}
