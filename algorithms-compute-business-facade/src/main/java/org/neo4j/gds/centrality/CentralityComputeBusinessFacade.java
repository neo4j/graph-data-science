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
import org.neo4j.gds.articulationPoints.ArticulationPointsParameters;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.betweenness.BetweennessCentralityParameters;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.BridgesParameters;
import org.neo4j.gds.centrality.validation.BetweennessCentralityGraphStoreValidation;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;
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

    public <TR> CompletableFuture<TR> articulationPoints(
        GraphName graphName,
        GraphParameters graphParameters,
        ArticulationPointsParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<ArticulationPointsResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(
                new UndirectedOnlyRequirement("Articulation Points")
            ),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.articulationPoints(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> betweennessCentrality(
        GraphName graphName,
        GraphParameters graphParameters,
        BetweennessCentralityParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<BetwennessCentralityResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(
                new BetweennessCentralityGraphStoreValidation()
            ),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.betweennessCentrality(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> bridges(
        GraphName graphName,
        GraphParameters graphParameters,
        BridgesParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<BridgeResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(
                new UndirectedOnlyRequirement("Bridges")
            ),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.bridges(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }


}
