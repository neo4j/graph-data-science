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

import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.articulationPoints.ArticulationPointsParameters;
import org.neo4j.gds.articulationpoints.ArticulationPoints;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentrality;
import org.neo4j.gds.betweenness.BetweennessCentralityParameters;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.Bridges;
import org.neo4j.gds.bridges.BridgesParameters;
import org.neo4j.gds.closeness.ClosenessCentrality;
import org.neo4j.gds.closeness.ClosenessCentralityParameters;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
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
import org.neo4j.gds.termination.TerminationFlag;

import static org.neo4j.gds.pagerank.PageRankVariant.ARTICLE_RANK;
import static org.neo4j.gds.pagerank.PageRankVariant.EIGENVECTOR;
import static org.neo4j.gds.pagerank.PageRankVariant.PAGE_RANK;

public class CentralityAlgorithms {

    private final TerminationFlag terminationFlag;

    public CentralityAlgorithms(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
    }

    public PageRankResult articleRank(Graph graph, ArticleRankConfig configuration, ProgressTracker progressTracker) {
        var articleRankComputation = articleRankComputation(graph, configuration);

        var articleRank = new PageRankAlgorithm<>(
            graph,
            configuration,
            articleRankComputation,
            ARTICLE_RANK,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return articleRank.compute();
    }

    ArticulationPointsResult articulationPoints(
        Graph graph,
        ArticulationPointsParameters parameters,
        ProgressTracker progressTracker
    ) {
        return ArticulationPoints
            .create(graph, parameters, progressTracker,terminationFlag)
            .compute();
    }


    public BetwennessCentralityResult betweennessCentrality(
        Graph graph,
        BetweennessCentralityParameters parameters,
        ProgressTracker progressTracker
    ) {

        return BetweennessCentrality
            .create(graph, parameters, progressTracker, terminationFlag)
            .compute();
    }

    BridgeResult bridges(Graph graph, BridgesParameters parameters, ProgressTracker progressTracker) {

        return Bridges
            .create(graph, progressTracker, parameters.computeComponents(),terminationFlag)
            .compute();
    }

    public CELFResult celf(Graph graph, CELFParameters parameters, ProgressTracker progressTracker) {
        return new CELF(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        ).compute();
    }

    public ClosenessCentralityResult closenessCentrality(
        Graph graph,
        ClosenessCentralityParameters parameters,
        ProgressTracker progressTracker
    ) {
        return ClosenessCentrality
            .create(graph, parameters, DefaultPool.INSTANCE, progressTracker, terminationFlag)
            .compute();
    }

    DegreeCentralityResult degreeCentrality(
        Graph graph,
        DegreeCentralityParameters parameters,
        ProgressTracker progressTracker
    ) {

        var algorithm = new DegreeCentrality(
            graph,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            parameters.orientation(),
            parameters.hasRelationshipWeightProperty(),
            parameters.minBatchSize(),
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    public PageRankResult eigenVector(
        Graph graph,
        EigenvectorConfig configuration,
        ProgressTracker progressTracker
    ) {
        var eigenvectorComputation = eigenvectorComputation(graph, configuration);

        var eigenvector = new PageRankAlgorithm<>(
            graph,
            configuration,
            eigenvectorComputation,
            EIGENVECTOR,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return eigenvector.compute();
    }


    public HarmonicResult harmonicCentrality(
        Graph graph,
        HarmonicCentralityParameters parameters,
        ProgressTracker progressTracker
    ) {
        var algorithm = new HarmonicCentrality(
            graph,
            parameters.concurrency(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    PregelResult hits(Graph graph, HitsConfig configuration, ProgressTracker progressTracker) {

        var algorithm = new Hits(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    IndirectExposureResult indirectExposure(
        Graph graph,
        IndirectExposureConfig configuration,
        ProgressTracker progressTracker
    ) {

        var algorithm = new IndirectExposure(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }


    public PageRankResult pageRank(Graph graph, PageRankConfig configuration, ProgressTracker progressTracker) {
        var pageRankComputation = pageRankComputation(graph, configuration);

        var pageRank = new PageRankAlgorithm<>(
            graph,
            configuration,
            pageRankComputation,
            PAGE_RANK,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return pageRank.compute();
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

    private PageRankComputation<PageRankConfig> pageRankComputation(Graph graph, PageRankConfig configuration) {
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
