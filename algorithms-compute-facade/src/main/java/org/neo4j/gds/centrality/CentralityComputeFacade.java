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

import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.gds.CentralityAlgorithmTasks;
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.articulationPoints.ArticulationPointsParameters;
import org.neo4j.gds.articulationpoints.ArticulationPoints;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.beta.pregel.ImmutablePregelResult;
import org.neo4j.gds.beta.pregel.NodeValue;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.betweenness.BetweennessCentrality;
import org.neo4j.gds.betweenness.BetweennessCentralityParameters;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.Bridges;
import org.neo4j.gds.bridges.BridgesParameters;
import org.neo4j.gds.closeness.ClosenessCentrality;
import org.neo4j.gds.closeness.ClosenessCentralityParameters;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.DegreeCentralityParameters;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentrality;
import org.neo4j.gds.harmonic.HarmonicCentralityParameters;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.Hits;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.indirectExposure.IndirectExposure;
import org.neo4j.gds.indirectExposure.IndirectExposureConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;
import org.neo4j.gds.influenceMaximization.CELF;
import org.neo4j.gds.influenceMaximization.CELFParameters;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.pagerank.ArticleRankComputation;
import org.neo4j.gds.pagerank.ArticleRankConfig;
import org.neo4j.gds.pagerank.DegreeFunctions;
import org.neo4j.gds.pagerank.EigenvectorComputation;
import org.neo4j.gds.pagerank.EigenvectorConfig;
import org.neo4j.gds.pagerank.InitialProbabilityFactory;
import org.neo4j.gds.pagerank.InitialProbabilityProvider;
import org.neo4j.gds.pagerank.PageRankAlgorithm;
import org.neo4j.gds.pagerank.PageRankComputation;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.CompletableFuture;

import static org.neo4j.gds.pagerank.PageRankVariant.ARTICLE_RANK;
import static org.neo4j.gds.pagerank.PageRankVariant.EIGENVECTOR;
import static org.neo4j.gds.pagerank.PageRankVariant.PAGE_RANK;

public class CentralityComputeFacade {
    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;

    // Request scope dependencies
    private final TerminationFlag terminationFlag;

    // Local dependencies
    private final CentralityAlgorithmTasks tasks = new CentralityAlgorithmTasks();

    public CentralityComputeFacade(
        AsyncAlgorithmCaller algorithmCaller,
        ProgressTrackerFactory progressTrackerFactory,
        TerminationFlag terminationFlag
    ) {
        this.algorithmCaller = algorithmCaller;
        this.progressTrackerFactory = progressTrackerFactory;
        this.terminationFlag = terminationFlag;
    }

    public CompletableFuture<TimedAlgorithmResult<PageRankResult>> articleRank(
        Graph graph,
        ArticleRankConfig configuration,
        JobId jobId,
        boolean logProgress
        ) {

        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PageRankResult.EMPTY));
        }

        var articleRankComputation = articleRankComputation(graph, configuration);

        var progressTracker = progressTrackerFactory.create(
            tasks.articleRank(graph,configuration),
            jobId,
            configuration.concurrency(),
            logProgress
        );

        var articleRank = new PageRankAlgorithm<>(
            graph,
            configuration,
            articleRankComputation,
            ARTICLE_RANK,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            articleRank::compute,
            jobId
        );
    }

    private ArticleRankComputation<ArticleRankConfig> articleRankComputation(
        Graph graph,
        ArticleRankConfig configuration
    ) {
        var degreeFunction = DegreeFunctions.pageRankDegreeFunction(
            graph,
            configuration.hasRelationshipWeightProperty(),
            configuration.concurrency(),
            terminationFlag
        );

        var alpha = 1 - configuration.dampingFactor();
        InitialProbabilityProvider probabilityProvider = InitialProbabilityFactory.create(
            graph::toMappedNodeId,
            alpha,
            configuration.sourceNodes()
        );

        double avgDegree = DegreeFunctions.averageDegree(graph, configuration.concurrency());
        return new ArticleRankComputation<>(configuration, probabilityProvider, degreeFunction, avgDegree);
    }

    public CompletableFuture<TimedAlgorithmResult<ArticulationPointsResult>> articulationPoints(
        Graph graph,
        ArticulationPointsParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ArticulationPointsResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.articulationPoints(graph),
            jobId,
            parameters.concurrency(),
            logProgress
        );
        var articulationPoints = ArticulationPoints.create(graph, parameters, progressTracker,terminationFlag);

        return algorithmCaller.run(
            articulationPoints::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<BetwennessCentralityResult>> betweennessCentrality(
        Graph graph,
        BetweennessCentralityParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(BetwennessCentralityResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.betweennessCentrality(graph,parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var betweennessCentrality =  BetweennessCentrality
            .create(graph, parameters, progressTracker, terminationFlag);

        return algorithmCaller.run(
            betweennessCentrality::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<BridgeResult>> bridges(
        Graph graph,
        BridgesParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(BridgeResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.bridges(graph),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var bridges =  Bridges
            .create(graph, progressTracker, parameters.computeComponents(),terminationFlag);

        return algorithmCaller.run(
            bridges::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<CELFResult>> celf(
        Graph graph,
        CELFParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(CELFResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.CELF(graph,parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var celf =  new CELF(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            celf::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<ClosenessCentralityResult>> closeness(
        Graph graph,
        ClosenessCentralityParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ClosenessCentralityResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.closenessCentrality(graph),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var closeness = ClosenessCentrality.create(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            closeness::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<DegreeCentralityResult>> degree(
        Graph graph,
        DegreeCentralityParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(DegreeCentralityResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.degreeCentrality(graph),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var degree  = new DegreeCentrality(
            graph,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            parameters.orientation(),
            parameters.hasRelationshipWeightProperty(),
            parameters.minBatchSize(),
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            degree::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PageRankResult>> eigenVector(
        Graph graph,
        EigenvectorConfig configuration,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PageRankResult.EMPTY));
        }

        var eigenvectorComputation = eigenvectorComputation(graph, configuration);

        var progressTracker = progressTrackerFactory.create(
            tasks.eigenVector(graph,configuration),
            jobId,
            configuration.concurrency(),
            logProgress
        );

        var eigenvector = new PageRankAlgorithm<>(
            graph,
            configuration,
            eigenvectorComputation,
            EIGENVECTOR,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            eigenvector::compute,
            jobId
        );
    }

    private EigenvectorComputation<EigenvectorConfig> eigenvectorComputation(
        Graph graph,
        EigenvectorConfig configuration
    ) {
        var mappedSourceNodes = new LongScatterSet(configuration.sourceNodes().inputNodes().size());
        configuration.sourceNodes().inputNodes().stream()
            .mapToLong(graph::toMappedNodeId)
            .forEach(mappedSourceNodes::add);

        boolean hasRelationshipWeightProperty = configuration.hasRelationshipWeightProperty();
        Concurrency concurrency = configuration.concurrency();
        var degreeFunction = DegreeFunctions.eigenvectorDegreeFunction(
            graph,
            hasRelationshipWeightProperty,
            concurrency,
            terminationFlag
        );

        return new EigenvectorComputation<>(
            graph.nodeCount(),
            configuration,
            mappedSourceNodes,
            degreeFunction
        );
    }

    public CompletableFuture<TimedAlgorithmResult<HarmonicResult>> harmonic(
        Graph graph,
        HarmonicCentralityParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(HarmonicResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.harmonicCentrality(),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var harmonic = new HarmonicCentrality(
            graph,
            parameters.concurrency(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            harmonic::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PregelResult>> hits(
        Graph graph,
        HitsConfig configuration,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            var empty = NodeValue.of(new PregelSchema.Builder().build(),0,configuration.concurrency());
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ImmutablePregelResult.of(empty, 0, false)));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.hits(graph,configuration),
            jobId,
            configuration.concurrency(),
            logProgress
        );

        var hits = new Hits(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmCaller.run(
            hits::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<IndirectExposureResult>> indirectExposure(
        Graph graph,
        IndirectExposureConfig configuration,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(IndirectExposureResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.indirectExposure(graph,configuration),
            jobId,
            configuration.concurrency(),
            logProgress
        );

        var indirectExposure = new IndirectExposure(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmCaller.run(
            indirectExposure::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PageRankResult>> pageRank(
        Graph graph,
        PageRankConfig configuration,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()){
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PageRankResult.EMPTY));
        }

        var pageRankComputation = pagerankComputation(graph, configuration);

        var progressTracker = progressTrackerFactory.create(
            tasks.pageRank(graph,configuration),
            jobId,
            configuration.concurrency(),
            logProgress
        );

        var pageRank = new PageRankAlgorithm<>(
            graph,
            configuration,
            pageRankComputation,
            PAGE_RANK,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            pageRank::compute,
            jobId
        );
    }

    private PageRankComputation<PageRankConfig> pagerankComputation(
        Graph graph,
        PageRankConfig configuration
    ) {
        var degreeFunction = DegreeFunctions.pageRankDegreeFunction(
            graph,
            configuration.hasRelationshipWeightProperty(),
            configuration.concurrency(),
            terminationFlag
        );

        var alpha = 1 - configuration.dampingFactor();
        InitialProbabilityProvider probabilityProvider = InitialProbabilityFactory.create(
            graph::toMappedNodeId,
            alpha,
            configuration.sourceNodes()
        );
        return new PageRankComputation<>(configuration, probabilityProvider, degreeFunction);
    }


}
