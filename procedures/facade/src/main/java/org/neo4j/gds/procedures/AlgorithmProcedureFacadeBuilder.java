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

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.embeddings.NodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.miscellaneous.MiscellaneousProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingProcedureFacade;
import org.neo4j.gds.procedures.algorithms.runners.AlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

class AlgorithmProcedureFacadeBuilder {
    private final RequestScopedDependencies requestScopedDependencies;
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final NodeLookup nodeLookup;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final ApplicationsFacade applicationsFacade;
    private final GenericStub genericStub;
    private final EstimationModeRunner estimationModeRunner;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;

    AlgorithmProcedureFacadeBuilder(
        RequestScopedDependencies requestScopedDependencies,
        CloseableResourceRegistry closeableResourceRegistry,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns,
        ApplicationsFacade applicationsFacade,
        GenericStub genericStub,
        EstimationModeRunner estimationModeRunner,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding)
    {
        this.requestScopedDependencies = requestScopedDependencies;
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.nodeLookup = nodeLookup;
        this.procedureReturnColumns = procedureReturnColumns;
        this.applicationsFacade = applicationsFacade;
        this.genericStub = genericStub;
        this.estimationModeRunner = estimationModeRunner;
        this.algorithmExecutionScaffolding = algorithmExecutionScaffolding;
    }

    CentralityProcedureFacade createCentralityProcedureFacade() {
        return CentralityProcedureFacade.create(
            genericStub,
            applicationsFacade,
            procedureReturnColumns,
            estimationModeRunner,
            algorithmExecutionScaffolding
        );
    }

    CommunityProcedureFacade createCommunityProcedureFacade() {
        return CommunityProcedureFacade.create(
            genericStub,
            applicationsFacade,
            closeableResourceRegistry,
            procedureReturnColumns,
            estimationModeRunner,
            algorithmExecutionScaffolding
        );
    }

    MiscellaneousProcedureFacade createMiscellaneousProcedureFacade() {
        return MiscellaneousProcedureFacade.create(
            genericStub,
            applicationsFacade,
            procedureReturnColumns,
            estimationModeRunner,
            algorithmExecutionScaffolding
        );
    }

    NodeEmbeddingsProcedureFacade createNodeEmbeddingsProcedureFacade() {
        return NodeEmbeddingsProcedureFacade.create(
            requestScopedDependencies,
            genericStub,
            applicationsFacade,
            estimationModeRunner,
            algorithmExecutionScaffolding
        );
    }

    PathFindingProcedureFacade createPathFindingProcedureFacade() {
        return PathFindingProcedureFacade.create(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns,
            applicationsFacade,
            genericStub,
            estimationModeRunner,
            algorithmExecutionScaffolding
        );
    }

    SimilarityProcedureFacade createSimilarityProcedureFacade() {
        return SimilarityProcedureFacade.create(
            applicationsFacade,
            genericStub,
            procedureReturnColumns,
            estimationModeRunner,
            algorithmExecutionScaffolding
        );
    }
}
