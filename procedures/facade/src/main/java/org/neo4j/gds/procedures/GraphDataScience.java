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
import org.neo4j.gds.procedures.embeddings.NodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.similarity.SimilarityProcedureFacade;

public class GraphDataScience {
    private final Log log;
    private final CatalogFacade catalogFacade;
    private final CommunityProcedureFacade communityProcedureFacade;
    private final CentralityProcedureFacade centralityProcedureFacade;
    private final SimilarityProcedureFacade similarityProcedureFacade;
    private final NodeEmbeddingsProcedureFacade nodeEmbeddingsProcedureFacade;
    private final DeprecatedProceduresMetricService deprecatedProceduresMetricService;

    public GraphDataScience(
        Log log, CatalogFacade catalogFacade,
        CentralityProcedureFacade centralityProcedureFacade,
        CommunityProcedureFacade communityProcedureFacade,
        NodeEmbeddingsProcedureFacade nodeEmbeddingsProcedureFacade,
        SimilarityProcedureFacade similarityProcedureFacade,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService
    ) {
        this.log = log;
        this.catalogFacade = catalogFacade;
        this.centralityProcedureFacade = centralityProcedureFacade;
        this.communityProcedureFacade = communityProcedureFacade;
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

    public SimilarityProcedureFacade similarity() {
        return similarityProcedureFacade;
    }

    public NodeEmbeddingsProcedureFacade nodeEmbeddings() {
        return nodeEmbeddingsProcedureFacade;
    }


    public DeprecatedProceduresMetricService deprecatedProcedures() {
        return deprecatedProceduresMetricService;
    }
}
