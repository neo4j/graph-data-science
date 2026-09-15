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

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultRenderer;
import org.neo4j.gds.articulationpoints.ArticulationPointsBaseConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamConfig;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.BridgesStreamConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.degree.DegreeCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityBaseConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStreamConfig;
import org.neo4j.gds.pagerank.ArticleRankStreamConfig;
import org.neo4j.gds.pagerank.EigenvectorStreamConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankStreamConfig;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticleRank;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.CELF;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ClosenessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.DegreeCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.EigenVector;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PageRank;

public final class CentralityAlgorithmsStreamModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final TrackedCentralityAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final InstrumentedCentralityAlgorithms instrumentedCentralityAlgorithms;
    private final Synchroniser synchroniser;

    CentralityAlgorithmsStreamModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        TrackedCentralityAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        InstrumentedCentralityAlgorithms instrumentedCentralityAlgorithms,
        Synchroniser synchroniser
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.instrumentedCentralityAlgorithms = instrumentedCentralityAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> Stream<RESULT> articleRank(
        GraphName graphName,
        ArticleRankStreamConfig configuration,
        StreamResultBuilder<PageRankResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ArticleRank,
            estimationFacade::pageRank,
            (graph, __) -> algorithms.articleRank(graph, configuration),
            streamResultBuilder
        );
    }

    /**
     * Stream mode means using certain result handling, and appearing in synchronous mode/ terminating asynchronous mode
     */
    public <RESULT> Stream<RESULT> articulationPoints(
        GraphName graphName,
        ArticulationPointsBaseConfig configuration,
        StreamResultBuilder<ArticulationPointsResult, RESULT> resultBuilder,
        boolean shouldComputeComponents
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.articulationPoints(
            graphName,
            configuration,
            Optional.empty(), // this is the value add for this layer
            new StreamResultRenderer<>(resultBuilder), // and this
            shouldComputeComponents
        ));
    }

    public <RESULT> Stream<RESULT> betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityStreamConfig configuration,
        StreamResultBuilder<CentralityAlgorithmResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            BetweennessCentrality,
            () -> estimationFacade.betweennessCentrality(configuration),
            (graph, __) -> algorithms.betweennessCentrality(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> bridges(
        GraphName graphName,
        BridgesStreamConfig configuration,
        StreamResultBuilder<BridgeResult, RESULT> resultBuilder,
        boolean shouldComputeComponents
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.bridges(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder),
            shouldComputeComponents
        ));
    }

    public <RESULT> Stream<RESULT> celf(
        GraphName graphName,
        InfluenceMaximizationStreamConfig configuration,
        StreamResultBuilder<CELFResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            CELF,
            () -> estimationFacade.celf(configuration),
            (graph, __) -> algorithms.celf(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> closenessCentrality(
        GraphName graphName,
        ClosenessCentralityStreamConfig configuration,
        StreamResultBuilder<CentralityAlgorithmResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ClosenessCentrality,
            estimationFacade::closenessCentrality,
            (graph, __) -> algorithms.closenessCentrality(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> degreeCentrality(
        GraphName graphName,
        DegreeCentralityStreamConfig configuration,
        StreamResultBuilder<CentralityAlgorithmResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            DegreeCentrality,
            () -> estimationFacade.degreeCentrality(configuration),
            (graph, __) -> algorithms.degreeCentrality(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> eigenvector(
        GraphName graphName,
        EigenvectorStreamConfig configuration,
        StreamResultBuilder<PageRankResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            EigenVector,
            estimationFacade::pageRank,
            (graph, __) -> algorithms.eigenVector(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityBaseConfig configuration,
        StreamResultBuilder<HarmonicResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.harmonicCentrality(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> hits(
        GraphName graphName,
        HitsConfig configuration,
        StreamResultBuilder<PregelResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.hits(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> pageRank(
        GraphName graphName,
        PageRankStreamConfig configuration,
        StreamResultBuilder<PageRankResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            PageRank,
            estimationFacade::pageRank,
            (graph, __) -> algorithms.pageRank(graph, configuration),
            streamResultBuilder
        );
    }
}
