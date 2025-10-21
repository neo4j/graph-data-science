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
package org.neo4j.gds.community;

import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutParameters;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.community.validation.MinCommunitySizeSumRequirement;
import org.neo4j.gds.community.validation.SpeakerListenerLPAGraphStoreRequirements;
import org.neo4j.gds.community.validation.TriangleCountGraphStoreRequirements;
import org.neo4j.gds.conductance.ConductanceParameters;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.AlgorithmGraphStoreRequirementsBuilder;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.NoAlgorithmRequirements;
import org.neo4j.gds.core.loading.validation.NodePropertyMustExistOnAllLabels;
import org.neo4j.gds.core.loading.validation.NodePropertyMustExistOnAnyLabel;
import org.neo4j.gds.core.loading.validation.NodePropertyTypeRequirement;
import org.neo4j.gds.core.loading.validation.SeedPropertyGraphStoreRequirement;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.hdbscan.HDBScanParameters;
import org.neo4j.gds.hdbscan.Labels;
import org.neo4j.gds.k1coloring.K1ColoringParameters;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.kcore.KCoreDecompositionParameters;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kmeans.KmeansParameters;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.labelpropagation.LabelPropagationParameters;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.leiden.LeidenParameters;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.louvain.LouvainParameters;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularity.ModularityParameters;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationParameters;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;
import org.neo4j.gds.scc.SccParameters;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientParameters;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountParameters;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleResult;
import org.neo4j.gds.wcc.WccParameters;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class CommunityComputeBusinessFacade {

    // Global dependencies
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final CommunityComputeFacade computeFacade;

    // Request scope dependencies -- can we move these as method parameters?! 🤔
    private final User user;
    private final DatabaseId databaseId;

    public CommunityComputeBusinessFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        CommunityComputeFacade computeFacade,
        User user,
        DatabaseId databaseId
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.computeFacade = computeFacade;
        this.user = user;
        this.databaseId = databaseId;
    }

    public <TR> CompletableFuture<TR> approxMaxKCut(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        ApproxMaxKCutParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<ApproxMaxKCutResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new MinCommunitySizeSumRequirement(parameters.minCommunitySizes())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.approxMaxKCut(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> cliqueCounting(
        GraphName graphName,
        GraphParameters graphParameters,
        CliqueCountingParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<CliqueCountingResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new UndirectedOnlyRequirement("Clique Counting")),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.cliqueCounting(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> conductance(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        ConductanceParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<ConductanceResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new NodePropertyMustExistOnAnyLabel("communityProperty")),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.conductance(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> hdbscan(
        GraphName graphName,
        GraphParameters graphParameters,
        HDBScanParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<Labels>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new NodePropertyMustExistOnAnyLabel("nodeProperty")),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.hdbscan(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> k1Coloring(
        GraphName graphName,
        GraphParameters graphParameters,
        K1ColoringParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<K1ColoringResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new NoAlgorithmRequirements()),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.k1Coloring(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> kCoreDecomposition(
        GraphName graphName,
        GraphParameters graphParameters,
        KCoreDecompositionParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<KCoreDecompositionResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new UndirectedOnlyRequirement("K-Core-Decomposition")),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.kCore(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> kMeans(
        GraphName graphName,
        GraphParameters graphParameters,
        KmeansParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<KmeansResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new NodePropertyTypeRequirement(
                "nodeProperty",
                List.of(ValueType.DOUBLE_ARRAY, ValueType.FLOAT_ARRAY, ValueType.DOUBLE)
            )),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.kMeans(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> labelPropagation(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        LabelPropagationParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<LabelPropagationResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new AlgorithmGraphStoreRequirementsBuilder()
                .withAlgorithmRequirement(SeedPropertyGraphStoreRequirement.create(Optional.ofNullable(parameters.seedProperty())))
                .withAlgorithmRequirement(new NodePropertyMustExistOnAllLabels(parameters.nodeWeightProperty()))
                .build(),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.labelPropagation(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> lcc(
        GraphName graphName,
        GraphParameters graphParameters,
        LocalClusteringCoefficientParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<LocalClusteringCoefficientResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new AlgorithmGraphStoreRequirementsBuilder()
                .withAlgorithmRequirement(new UndirectedOnlyRequirement("LocalClusteringCoefficient"))
                .withAlgorithmRequirement(SeedPropertyGraphStoreRequirement.create(Optional.ofNullable(parameters.seedProperty())))
                .build(),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.lcc(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> leiden(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        LeidenParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<LeidenResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new AlgorithmGraphStoreRequirementsBuilder()
                .withAlgorithmRequirement(new UndirectedOnlyRequirement("Leiden"))
                .withAlgorithmRequirement(SeedPropertyGraphStoreRequirement.create(Optional.ofNullable(parameters.seedProperty())))
                .build(),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.leiden(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> louvain(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        LouvainParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<LouvainResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(SeedPropertyGraphStoreRequirement.create(Optional.ofNullable(parameters.seedProperty()))),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.louvain(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> modularity(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        ModularityParameters parameters,
        JobId jobId,
        ResultTransformerBuilder<TimedAlgorithmResult<ModularityResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new NodePropertyMustExistOnAnyLabel("communityProperty")),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.modularity(
            graph,
            parameters,
            jobId

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> modularityOptimization(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        ModularityOptimizationParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<ModularityOptimizationResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(SeedPropertyGraphStoreRequirement.create(parameters.seedProperty())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.modularityOptimization(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> scc(
        GraphName graphName,
        GraphParameters graphParameters,
        SccParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<HugeLongArray>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new NoAlgorithmRequirements()),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.scc(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> sllpa(
        GraphName graphName,
        GraphParameters graphParameters,
        SpeakerListenerLPAConfig config,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PregelResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new SpeakerListenerLPAGraphStoreRequirements(config.writeProperty())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.sllpa(
            graph,
            config,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> triangleCount(
        GraphName graphName,
        GraphParameters graphParameters,
        TriangleCountParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<TriangleCountResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(TriangleCountGraphStoreRequirements.create(parameters.labelFilter())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.triangleCount(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> triangles(
        GraphName graphName,
        GraphParameters graphParameters,
        TriangleCountParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<Stream<TriangleResult>>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(TriangleCountGraphStoreRequirements.create(parameters.labelFilter())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.triangles(
            graph,
            parameters,
            jobId
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> wcc(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        WccParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<DisjointSetStruct>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(SeedPropertyGraphStoreRequirement.create(parameters.seedProperty())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.wcc(
            graph,
            parameters,
            jobId,
            logProgress

        ).thenApply(resultTransformerBuilder.build(graphResources));
    }


}
