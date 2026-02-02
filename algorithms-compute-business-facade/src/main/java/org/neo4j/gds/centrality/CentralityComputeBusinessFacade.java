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
import org.neo4j.gds.closeness.ClosenessCentralityParameters;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.AlgorithmGraphStoreRequirementsBuilder;
import org.neo4j.gds.core.loading.validation.DirectedOnlyRequirement;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.NoAlgorithmRequirements;
import org.neo4j.gds.core.loading.validation.PregelPropertiesRequirement;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;
import org.neo4j.gds.degree.DegreeCentralityParameters;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentralityParameters;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsCompanion;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.hits.HitsResultWithGraph;
import org.neo4j.gds.indirectExposure.IndirectExposureConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;
import org.neo4j.gds.influenceMaximization.CELFParameters;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.pagerank.ArticleRankConfig;
import org.neo4j.gds.pagerank.EigenvectorConfig;
import org.neo4j.gds.pagerank.PageRankConfig;
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
        Optional<String> relationshipProperty,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<BetwennessCentralityResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
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

    public <TR> CompletableFuture<TR> celf(
        GraphName graphName,
        GraphParameters graphParameters,
        CELFParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<CELFResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(
                new NoAlgorithmRequirements()
            ),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.celf(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> closeness(
        GraphName graphName,
        GraphParameters graphParameters,
        ClosenessCentralityParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<ClosenessCentralityResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(
                new NoAlgorithmRequirements()
            ),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.closeness(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> degree(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        DegreeCentralityParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<DegreeCentralityResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(
                new NoAlgorithmRequirements()
            ),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.degree(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> eigenVector(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        EigenvectorConfig config,
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

        return computeFacade.eigenVector(
            graph,
            config,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> harmonic(
        GraphName graphName,
        GraphParameters graphParameters,
        HarmonicCentralityParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<HarmonicResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(
                new NoAlgorithmRequirements()
            ),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.harmonic(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> indirectExposure(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        IndirectExposureConfig config,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<IndirectExposureResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(
                new UndirectedOnlyRequirement("Indirect exposure")
            ),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.indirectExposure(
            graph,
            config,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> pageRank(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        PageRankConfig config,
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

        return computeFacade.pageRank(
            graph,
            config,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> hits(
        GraphName graphName,
        GraphParameters graphParameters,
        HitsConfig hitsConfig,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<HitsResultWithGraph>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphStoreOnlyResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new AlgorithmGraphStoreRequirementsBuilder()
                .withAlgorithmRequirement(new PregelPropertiesRequirement(hitsConfig.writeProperty()))
                .withAlgorithmRequirement(new DirectedOnlyRequirement("Hits"))
                .build(),
            user,
            databaseId
        );
        var graphStore = graphResources.graphStore();
        var relTypes = HitsCompanion.relationshipsWithoutIndices(
            graphStore,
            hitsConfig.internalRelationshipTypes(graphStore)
        );

        return computeFacade.hits(
            graphStore,
            hitsConfig,
            relTypes,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }



}
