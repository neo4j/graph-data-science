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
package org.neo4j.gds.centrality;

import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.pagerank.ArticleRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class CentralityComputeBusinessFacade {

    // Global dependencies
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final CentralityComputeFacade computeFacade;

    // Request scope dependencies -- can we move these as method parameters?! 🤔
    private final User user;
    private final DatabaseId databaseId;

    public CentralityComputeBusinessFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        CentralityComputeFacade computeFacade,
        User user,
        DatabaseId databaseId
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.computeFacade = computeFacade;
        this.user = user;
        this.databaseId = databaseId;
    }

    public <TR> CompletableFuture<TR> articleRank(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        ArticleRankConfig config,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PageRankResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(
                new SourceNodesRequirement(config.sourceNodes().inputNodes())
            ),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.articleRank(
            graph,
            config,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

}
