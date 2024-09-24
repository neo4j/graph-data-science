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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.centrality.LocalCentralityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.embeddings.NodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.machinelearning.MachineLearningProcedureFacade;
import org.neo4j.gds.procedures.algorithms.miscellaneous.MiscellaneousProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingProcedureFacade;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.embeddings.LocalNodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.LocalPathFindingProcedureFacade;
import org.neo4j.gds.procedures.algorithms.similarity.LocalSimilarityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.kernel.api.KernelTransaction;

/**
 * Just some squirreled away scaffolding, but also,
 * hides some dependencies that you would otherwise need to make available to, for example,
 * a {@link org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade} factory method
 */
final class AlgorithmsProcedureFacadeFactory {
    private AlgorithmsProcedureFacadeFactory() {}

    static AlgorithmsProcedureFacade create(
        UserSpecificConfigurationParser configurationParser,
        RequestScopedDependencies requestScopedDependencies,
        KernelTransaction kernelTransaction,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns,
        AlgorithmEstimationTemplate algorithmEstimationTemplate
    ) {
        var nodeLookup = new TransactionNodeLookup(kernelTransaction);
        var closeableResourceRegistry = new TransactionCloseableResourceRegistry(kernelTransaction);

        var genericStub = new GenericStub(
            configurationParser,
            algorithmEstimationTemplate
        );

        var centralityProcedureFacade = LocalCentralityProcedureFacade.create(
            genericStub,
            applicationsFacade,
            procedureReturnColumns,
            configurationParser
        );

        var communityProcedureFacade = CommunityProcedureFacade.create(
            genericStub,
            applicationsFacade,
            closeableResourceRegistry,
            procedureReturnColumns,
            configurationParser
        );

        var machineLearningProcedureFacade = MachineLearningProcedureFacade.create(
            genericStub,
            applicationsFacade,
            configurationParser
        );

        var miscellaneousProcedureFacade = MiscellaneousProcedureFacade.create(
            genericStub,
            applicationsFacade,
            procedureReturnColumns,
            configurationParser
        );

        var nodeEmbeddingsProcedureFacade = LocalNodeEmbeddingsProcedureFacade.create(
            genericStub,
            applicationsFacade,
            configurationParser,
            requestScopedDependencies.getUser()
        );

    var pathFindingProcedureFacade = LocalPathFindingProcedureFacade.create(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns,
            applicationsFacade,
            genericStub,
            configurationParser
        );

        var similarityProcedureFacade = LocalSimilarityProcedureFacade.create(
            applicationsFacade,
            genericStub,
            procedureReturnColumns,
            configurationParser
        );

        return new AlgorithmsProcedureFacade(
            centralityProcedureFacade,
            communityProcedureFacade,
            machineLearningProcedureFacade,
            miscellaneousProcedureFacade,
            nodeEmbeddingsProcedureFacade,
            pathFindingProcedureFacade,
            similarityProcedureFacade
        );
    }
}
