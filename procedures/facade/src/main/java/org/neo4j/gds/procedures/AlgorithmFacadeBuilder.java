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
package org.neo4j.gds.procedures;

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsFacade;
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
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.modelcatalogservices.ModelCatalogService;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingProcedureFacade;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.runners.StatsModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.runners.StreamModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.runners.WriteModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.embeddings.NodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.misc.MiscAlgorithmsProcedureFacade;

class AlgorithmFacadeBuilder {
    // Request scoped parameters
    private final ConfigurationCreator configurationCreator;
    private final NodeLookup nodeLookup;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final WriteNodePropertyService writeNodePropertyService;
    private final AlgorithmEstimator algorithmEstimator;
    private final AlgorithmRunner algorithmRunner;
    private final ModelCatalogService modelCatalogService;
    private final ApplicationsFacade applicationsFacade;
    private final GenericStub genericStub;
    private final EstimationModeRunner estimationModeRunner;
    private final StreamModeAlgorithmRunner streamModeAlgorithmRunner;
    private final StatsModeAlgorithmRunner statsModeAlgorithmRunner;
    private final WriteModeAlgorithmRunner writeModeAlgorithmRunner;

    AlgorithmFacadeBuilder(
        ConfigurationCreator configurationCreator,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns,
        MutateNodePropertyService mutateNodePropertyService,
        WriteNodePropertyService writeNodePropertyService,
        AlgorithmRunner algorithmRunner,
        AlgorithmEstimator algorithmEstimator,
        ModelCatalogService modelCatalogService,
        ApplicationsFacade applicationsFacade,
        GenericStub genericStub,
        EstimationModeRunner estimationModeRunner,
        StreamModeAlgorithmRunner streamModeAlgorithmRunner,
        StatsModeAlgorithmRunner statsModeAlgorithmRunner,
        WriteModeAlgorithmRunner writeModeAlgorithmRunner
    ) {
        this.configurationCreator = configurationCreator;
        this.nodeLookup = nodeLookup;
        this.procedureReturnColumns = procedureReturnColumns;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.writeNodePropertyService = writeNodePropertyService;
        this.algorithmRunner = algorithmRunner;
        this.algorithmEstimator = algorithmEstimator;
        this.modelCatalogService = modelCatalogService;
        this.applicationsFacade = applicationsFacade;
        this.genericStub = genericStub;
        this.estimationModeRunner = estimationModeRunner;
        this.streamModeAlgorithmRunner = streamModeAlgorithmRunner;
        this.statsModeAlgorithmRunner = statsModeAlgorithmRunner;
        this.writeModeAlgorithmRunner = writeModeAlgorithmRunner;
    }

    CentralityProcedureFacade createCentralityProcedureFacade() {
        return CentralityProcedureFacade.create(
            genericStub,
            applicationsFacade,
            procedureReturnColumns,
            estimationModeRunner,
            statsModeAlgorithmRunner,
            streamModeAlgorithmRunner,
            writeModeAlgorithmRunner
        );
    }

    org.neo4j.gds.procedures.centrality.CentralityProcedureFacade createOldCentralityProcedureFacade() {

        // algorithm facade
        var centralityAlgorithmsFacade = new CentralityAlgorithmsFacade(algorithmRunner);

        var estimateBusinessFacade = new CentralityAlgorithmsEstimateBusinessFacade(algorithmEstimator);
        var statsBusinessFacade = new CentralityAlgorithmsStatsBusinessFacade(centralityAlgorithmsFacade);
        var streamBusinessFacade = new CentralityAlgorithmsStreamBusinessFacade(centralityAlgorithmsFacade);
        var writeBusinessFacade = new CentralityAlgorithmsWriteBusinessFacade(
            centralityAlgorithmsFacade,
            writeNodePropertyService
        );

        // procedure facade
        return new org.neo4j.gds.procedures.centrality.CentralityProcedureFacade(
            configurationCreator,
            procedureReturnColumns,
            estimateBusinessFacade,
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
            procedureReturnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );
    }

    SimilarityProcedureFacade createSimilarityProcedureFacade() {
        return SimilarityProcedureFacade.create(
            applicationsFacade,
            genericStub,
            procedureReturnColumns,
            estimationModeRunner,
            streamModeAlgorithmRunner,
            statsModeAlgorithmRunner,
            writeModeAlgorithmRunner
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
            procedureReturnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );
    }

    PathFindingProcedureFacade createPathFindingProcedureFacade() {
        return PathFindingProcedureFacade.create(
            nodeLookup,
            procedureReturnColumns,
            applicationsFacade,
            genericStub,
            estimationModeRunner,
            streamModeAlgorithmRunner,
            statsModeAlgorithmRunner,
            writeModeAlgorithmRunner
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
            procedureReturnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            trainBusinessFacade,
            writeBusinessFacade
        );
    }
}
