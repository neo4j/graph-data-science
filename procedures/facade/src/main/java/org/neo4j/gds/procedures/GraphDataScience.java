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

import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.procedures.catalog.CatalogFacade;
import org.neo4j.gds.procedures.centrality.CentralityProcedureFacade;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.misc.MiscAlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.pathfinding.PathFindingProcedureFacade;
import org.neo4j.gds.procedures.embeddings.NodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.pipelines.PipelinesProcedureFacade;
import org.neo4j.gds.procedures.similarity.SimilarityProcedureFacade;

public class GraphDataScience {
    private final Log log;

    private final AlgorithmsAndCatalogFacade algorithmsAndCatalogFacade;
    private final CatalogFacade catalogFacade;
    private final CentralityProcedureFacade centralityProcedureFacade;
    private final CommunityProcedureFacade communityProcedureFacade;
    private final MiscAlgorithmsProcedureFacade miscAlgorithmsProcedureFacade;
    private final NodeEmbeddingsProcedureFacade nodeEmbeddingsProcedureFacade;
    private final PathFindingProcedureFacade pathFindingProcedureFacade;
    private final PipelinesProcedureFacade pipelinesProcedureFacade;
    private final SimilarityProcedureFacade similarityProcedureFacade;

    private final DeprecatedProceduresMetricService deprecatedProceduresMetricService;

    /**
     * Keeping this package private to encourage use of @{@link org.neo4j.gds.procedures.GraphDataScienceBuilder}
     */
    GraphDataScience(
        Log log,
        AlgorithmsAndCatalogFacade algorithmsAndCatalogFacade,
        CatalogFacade catalogFacade,
        CentralityProcedureFacade centralityProcedureFacade,
        CommunityProcedureFacade communityProcedureFacade,
        MiscAlgorithmsProcedureFacade miscAlgorithmsProcedureFacade,
        NodeEmbeddingsProcedureFacade nodeEmbeddingsProcedureFacade,
        PathFindingProcedureFacade pathFindingProcedureFacade,
        PipelinesProcedureFacade pipelinesProcedureFacade,
        SimilarityProcedureFacade similarityProcedureFacade,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService
    ) {
        this.log = log;
        this.algorithmsAndCatalogFacade = algorithmsAndCatalogFacade;
        this.catalogFacade = catalogFacade;
        this.centralityProcedureFacade = centralityProcedureFacade;
        this.communityProcedureFacade = communityProcedureFacade;
        this.miscAlgorithmsProcedureFacade = miscAlgorithmsProcedureFacade;
        this.pathFindingProcedureFacade = pathFindingProcedureFacade;
        this.pipelinesProcedureFacade = pipelinesProcedureFacade;
        this.similarityProcedureFacade = similarityProcedureFacade;
        this.nodeEmbeddingsProcedureFacade = nodeEmbeddingsProcedureFacade;
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
    }

    public Log log() {
        return log;
    }

    public CatalogFacade catalog() {
        return catalogFacade;
    }

    public CentralityProcedureFacade centrality() {
        return centralityProcedureFacade;
    }

    public CommunityProcedureFacade community() {
        return communityProcedureFacade;
    }

    public MiscAlgorithmsProcedureFacade miscellaneousAlgorithms() {
        return miscAlgorithmsProcedureFacade;
    }

    public NodeEmbeddingsProcedureFacade nodeEmbeddings() {
        return nodeEmbeddingsProcedureFacade;
    }

    public PathFindingProcedureFacade pathFinding() {
        return pathFindingProcedureFacade;
    }

    public PipelinesProcedureFacade pipelines() {
        return pipelinesProcedureFacade;
    }

    public SimilarityProcedureFacade similarity() {
        return similarityProcedureFacade;
    }

    public DeprecatedProceduresMetricService deprecatedProcedures() {
        return deprecatedProceduresMetricService;
    }
}
