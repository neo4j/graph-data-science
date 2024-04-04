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
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmStatsBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsTrainBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.algorithms.misc.MiscAlgorithmMutateBusinessFacade;
import org.neo4j.gds.algorithms.misc.MiscAlgorithmStatsBusinessFacade;
import org.neo4j.gds.algorithms.misc.MiscAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.misc.MiscAlgorithmWriteBusinessFacade;
import org.neo4j.gds.algorithms.misc.MiscAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.misc.MiscAlgorithmsFacade;
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
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithms;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.modelcatalogservices.ModelCatalogService;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingProcedureFacade;
import org.neo4j.gds.procedures.centrality.CentralityProcedureFacade;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.embeddings.NodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.misc.MiscAlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.similarity.SimilarityProcedureFacade;
import org.neo4j.gds.termination.TerminationFlag;

class AlgorithmFacadeFactory {
    // Global scoped dependencies
    private final Log log;
    private final DefaultsConfiguration defaultsConfiguration;
    private final LimitsConfiguration limitsConfiguration;

    // Request scoped parameters
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final ConfigurationCreator configurationCreator;
    private final ConfigurationParser configurationParser;
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
    private final NodePropertyExporterBuilder nodePropertyExporterBuilder;
    private final RelationshipExporterBuilder relationshipExporterBuilder;
    private final RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;
    private final User user;
    private final UserLogRegistryFactory userLogRegistryFactory;
    private final ModelCatalogService modelCatalogService;

    AlgorithmFacadeFactory(
        Log log,
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        CloseableResourceRegistry closeableResourceRegistry,
        ConfigurationCreator configurationCreator,
        ConfigurationParser configurationParser,
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
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        RelationshipExporterBuilder relationshipExporterBuilder,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        User user,
        UserLogRegistryFactory userLogRegistryFactory,
        ModelCatalogService modelCatalogService
    ) {
        this.log = log;
        this.defaultsConfiguration = defaultsConfiguration;
        this.limitsConfiguration = limitsConfiguration;

        this.closeableResourceRegistry = closeableResourceRegistry;
        this.configurationCreator = configurationCreator;
        this.configurationParser = configurationParser;
        this.nodeLookup = nodeLookup;
        this.returnColumns = returnColumns;
        this.user = user;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.writeNodePropertyService = writeNodePropertyService;
        this.mutateRelationshipService = mutateRelationshipService;
        this.writeRelationshipService = writeRelationshipService;
        this.algorithmRunner = algorithmRunner;
        this.algorithmEstimator = algorithmEstimator;
        this.modelCatalogService = modelCatalogService;

        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
        this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
        this.relationshipExporterBuilder = relationshipExporterBuilder;
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

    MiscAlgorithmsProcedureFacade createMiscellaneousProcedureFacade() {

        // algorithm facade
        var miscAlgorithmsFacade = new MiscAlgorithmsFacade(algorithmRunner);

        var estimateBusinessFacade = new MiscAlgorithmsEstimateBusinessFacade(algorithmEstimator);

        var streamBusinessFacade = new MiscAlgorithmStreamBusinessFacade(miscAlgorithmsFacade);

        var statsBusinessFacade = new MiscAlgorithmStatsBusinessFacade(miscAlgorithmsFacade);

        var writeBusinessFacade = new MiscAlgorithmWriteBusinessFacade(miscAlgorithmsFacade, writeNodePropertyService);

        var mutateBusinessFacade = new MiscAlgorithmMutateBusinessFacade(
            miscAlgorithmsFacade,
            mutateNodePropertyService
        );


        // procedure facade
        return new MiscAlgorithmsProcedureFacade(
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

        var estimationModeFacade = new PathFindingAlgorithmsEstimationModeBusinessFacade(algorithmEstimationTemplate);

        var mutateModeFacade = new PathFindingAlgorithmsMutateModeBusinessFacade(
            estimationModeFacade,
            pathFindingAlgorithms,
            algorithmProcessingTemplate
        );

        /*
         * this guy will subsume ^^ shortly
         * then become a parameter
         */
        var applicationsFacade = ApplicationsFacade.create(
            log,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            relationshipStreamExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            algorithmProcessingTemplate,
            pathFindingAlgorithms,
            estimationModeFacade
        );

        return PathFindingProcedureFacade.create(
            defaultsConfiguration,
            limitsConfiguration,
            closeableResourceRegistry,
            configurationCreator,
            configurationParser,
            nodeLookup,
            returnColumns,
            user,
            estimationModeFacade,
            mutateModeFacade,
            applicationsFacade
        );
    }

    NodeEmbeddingsProcedureFacade createNodeEmbeddingsProcedureFacade() {
        // algorithms facade
        var nodeEmbeddingsAlgorithmsFacade = new NodeEmbeddingsAlgorithmsFacade(algorithmRunner, modelCatalogService);

        // mode-specific facades

        var mutateBusinessFacade = new NodeEmbeddingsAlgorithmsMutateBusinessFacade(
            nodeEmbeddingsAlgorithmsFacade,
            mutateNodePropertyService
        );

        var streamBusinessFacade = new NodeEmbeddingsAlgorithmStreamBusinessFacade(nodeEmbeddingsAlgorithmsFacade);

        var statsBusinessFacade = new NodeEmbeddingsAlgorithmStatsBusinessFacade(nodeEmbeddingsAlgorithmsFacade);

        var trainBusinessFacade = new NodeEmbeddingsAlgorithmsTrainBusinessFacade(
            nodeEmbeddingsAlgorithmsFacade,
            modelCatalogService
        );

        var writeBusinessFacade = new NodeEmbeddingsAlgorithmsWriteBusinessFacade(
            nodeEmbeddingsAlgorithmsFacade,
            writeNodePropertyService
        );

        var estimateBusinessFacade = new NodeEmbeddingsAlgorithmsEstimateBusinessFacade(
            algorithmEstimator,
            modelCatalogService
        );

        // procedure facade
        return new NodeEmbeddingsProcedureFacade(
            configurationCreator,
            returnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            trainBusinessFacade,
            writeBusinessFacade
        );
    }
}
