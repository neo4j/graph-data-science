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
package org.neo4j.gds.applications.algorithms.centrality;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.articulationpoints.ArticulationPointsBaseConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentralityBaseConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.BridgesStreamConfig;
import org.neo4j.gds.closeness.ClosenessCentralityBaseConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.loading.validation.DirectedOnlyRequirement;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.core.loading.validation.PregelPropertiesRequirement;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentralityBaseConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationBaseConfig;
import org.neo4j.gds.pagerank.ArticleRankConfig;
import org.neo4j.gds.pagerank.EigenvectorConfig;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * This is the place where we capture all the algorithm-specific choices that piece together an algorithm as a _piece of work_.
 * It is the only place where you declare a validation requirements-algorithm method-estimation method-label combo,
 * because anything else would be duplication.
 * Side effects and result rendering behaviours get injected as parameters.
 */
public final class InstrumentedCentralityAlgorithms {
    private final TrackedCentralityAlgorithms algorithms;
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;
    private final HitsHookGenerator hitsHookGenerator;

    private InstrumentedCentralityAlgorithms(
        TrackedCentralityAlgorithms algorithms,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience,
        HitsHookGenerator hitsHookGenerator
    ) {
        this.algorithms = algorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
        this.hitsHookGenerator = hitsHookGenerator;
    }

    public static InstrumentedCentralityAlgorithms create(
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        TerminationFlag terminationFlag
    ) {
        var algorithms = new CentralityAlgorithms(terminationFlag);
        var trackedAlgorithms = new TrackedCentralityAlgorithms(algorithms, progressTrackerCreator);

        var hitsHookGenerator = new HitsHookGenerator(progressTrackerCreator, terminationFlag);

        return new InstrumentedCentralityAlgorithms(
            trackedAlgorithms,
            estimationFacade,
            launchConvenience,
            hitsHookGenerator
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> articleRank(
        GraphName graphName,
        ArticleRankConfig configuration,
        Optional<SideEffect<PageRankResult, METADATA>> sideEffect,
        ResultRenderer<PageRankResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreFilteredETL(new SourceNodesRequirement(configuration.sourceNodes().inputNodes()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.articleRank(graph, configuration),
            estimationFacade::pageRank,
            AlgorithmLabel.ArticleRank,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> articulationPoints(
        GraphName graphName,
        ArticulationPointsBaseConfig configuration,
        Optional<SideEffect<ArticulationPointsResult, METADATA>> sideEffect,
        ResultRenderer<ArticulationPointsResult, RESULT, METADATA> resultRenderer,
        boolean shouldComputeComponents
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreFilteredETL(new UndirectedOnlyRequirement("Articulation Points"));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.articulationPoints(graph, configuration, shouldComputeComponents),
            () -> estimationFacade.articulationPoints(shouldComputeComponents),
            AlgorithmLabel.ArticulationPoints,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityBaseConfig configuration,
        Optional<SideEffect<BetwennessCentralityResult, METADATA>> sideEffect,
        ResultRenderer<BetwennessCentralityResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreFilteredETL(new BetweennessCentralityRequirements());

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.betweennessCentrality(graph, configuration),
            () -> estimationFacade.betweennessCentrality(configuration),
            AlgorithmLabel.BetweennessCentrality,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> bridges(
        GraphName graphName,
        BridgesStreamConfig configuration,
        Optional<SideEffect<BridgeResult, METADATA>> sideEffect,
        ResultRenderer<BridgeResult, RESULT, METADATA> resultRenderer,
        boolean shouldComputeComponents
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreFilteredETL(new UndirectedOnlyRequirement("Bridges"));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.bridges(graph, configuration, shouldComputeComponents),
            () -> estimationFacade.bridges(shouldComputeComponents),
            AlgorithmLabel.Bridges,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> celf(
        GraphName graphName,
        InfluenceMaximizationBaseConfig configuration,
        Optional<SideEffect<CELFResult, METADATA>> sideEffect,
        ResultRenderer<CELFResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.celf(graph, configuration),
            () -> estimationFacade.celf(configuration),
            AlgorithmLabel.CELF,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> closenessCentrality(
        GraphName graphName,
        ClosenessCentralityBaseConfig configuration,
        Optional<SideEffect<ClosenessCentralityResult, METADATA>> sideEffect,
        ResultRenderer<ClosenessCentralityResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.closenessCentrality(graph, configuration),
            estimationFacade::closenessCentrality,
            AlgorithmLabel.ClosenessCentrality,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> degreeCentrality(
        GraphName graphName,
        DegreeCentralityConfig configuration,
        Optional<SideEffect<DegreeCentralityResult, METADATA>> sideEffect,
        ResultRenderer<DegreeCentralityResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.degreeCentrality(graph, configuration),
            () -> estimationFacade.degreeCentrality(configuration),
            AlgorithmLabel.DegreeCentrality,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> eigenVector(
        GraphName graphName,
        EigenvectorConfig configuration,
        Optional<SideEffect<PageRankResult, METADATA>> sideEffect,
        ResultRenderer<PageRankResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreFilteredETL(new SourceNodesRequirement(configuration.sourceNodes().inputNodes()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.eigenVector(graph, configuration),
            estimationFacade::pageRank,
            AlgorithmLabel.EigenVector,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityBaseConfig configuration,
        Optional<SideEffect<HarmonicResult, METADATA>> sideEffect,
        ResultRenderer<HarmonicResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.harmonicCentrality(graph, configuration),
            estimationFacade::harmonicCentrality,
            AlgorithmLabel.HarmonicCentrality,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> hits(
        GraphName graphName,
        HitsConfig configuration,
        Optional<SideEffect<PregelResult, METADATA>> sideEffect,
        ResultRenderer<PregelResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreLoadedETL(hitsHookGenerator.generate(configuration)) // the hook creates inverse indexes
            .withGraphStoreFilteredETL(new PregelPropertiesRequirement(configuration.writeProperty(), configuration.writeToResultStore()))
            .withGraphStoreFilteredETL(new DirectedOnlyRequirement("Hits"));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.hits(graph, configuration),
            estimationFacade::hits,
            AlgorithmLabel.HITS,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> indirectExposure(
        GraphName graphName,
        IndirectExposureConfig configuration,
        Optional<SideEffect<IndirectExposureResult, METADATA>> sideEffect,
        ResultRenderer<IndirectExposureResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreFilteredETL(new UndirectedOnlyRequirement("Indirect exposure"));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.indirectExposure(graph, configuration),
            estimationFacade::indirectExposure,
            AlgorithmLabel.IndirectExposure,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> pageRank(
        GraphName graphName,
        PageRankConfig configuration,
        Optional<SideEffect<PageRankResult, METADATA>> sideEffect,
        ResultRenderer<PageRankResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreFilteredETL(new SourceNodesRequirement(configuration.sourceNodes().inputNodes()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.pageRank(graph, configuration),
            estimationFacade::pageRank,
            AlgorithmLabel.PageRank,
            sideEffect,
            resultRenderer
        );
    }
}
