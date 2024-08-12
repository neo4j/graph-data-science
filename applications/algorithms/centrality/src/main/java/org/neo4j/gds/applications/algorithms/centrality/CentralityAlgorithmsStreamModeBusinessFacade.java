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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.articulationpoints.ArticulationPointsStreamConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamConfig;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.BridgesStreamConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.degree.DegreeCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStreamConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankStreamConfig;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ArticleRank;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ArticulationPoints;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BRIDGES;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.CELF;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ClosenessCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.DegreeCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.EigenVector;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.HarmonicCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.PageRank;

public class CentralityAlgorithmsStreamModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CentralityAlgorithms centralityAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    CentralityAlgorithmsStreamModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CentralityAlgorithms centralityAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.estimationFacade = estimationFacade;
        this.centralityAlgorithms = centralityAlgorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> Stream<RESULT> articleRank(
        GraphName graphName,
        PageRankStreamConfig configuration,
        StreamResultBuilder<PageRankStreamConfig, PageRankResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ArticleRank,
            estimationFacade::pageRank,
            (graph, __) -> centralityAlgorithms.articleRank(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }

    public <RESULT> Stream<RESULT> betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityStreamConfig configuration,
        StreamResultBuilder<BetweennessCentralityStreamConfig, CentralityAlgorithmResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            BetweennessCentrality,
            () -> estimationFacade.betweennessCentrality(configuration),
            (graph, __) -> centralityAlgorithms.betweennessCentrality(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }
    public <RESULT> Stream<RESULT> articulationPoints(
        GraphName graphName,
        ArticulationPointsStreamConfig configuration,
        StreamResultBuilder<ArticulationPointsStreamConfig, BitSet, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ArticulationPoints,
            estimationFacade::articulationPoints,
            (graph, __) -> centralityAlgorithms.articulationPoints(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }
    public <RESULT> Stream<RESULT> bridges(
        GraphName graphName,
        BridgesStreamConfig configuration,
        StreamResultBuilder<BridgesStreamConfig, BridgeResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            BRIDGES,
            estimationFacade::bridges,
            (graph, __) -> centralityAlgorithms.bridges(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }

    public <RESULT> Stream<RESULT> celf(
        GraphName graphName,
        InfluenceMaximizationStreamConfig configuration,
        StreamResultBuilder<InfluenceMaximizationStreamConfig, CELFResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            CELF,
            () -> estimationFacade.celf(configuration),
            (graph, __) -> centralityAlgorithms.celf(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }

    public <RESULT> Stream<RESULT> closenessCentrality(
        GraphName graphName,
        ClosenessCentralityStreamConfig configuration,
        StreamResultBuilder<ClosenessCentralityStreamConfig, CentralityAlgorithmResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ClosenessCentrality,
            () -> estimationFacade.closenessCentrality(configuration),
            (graph, __) -> centralityAlgorithms.closenessCentrality(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }

    public <RESULT> Stream<RESULT> degreeCentrality(
        GraphName graphName,
        DegreeCentralityStreamConfig configuration,
        StreamResultBuilder<DegreeCentralityStreamConfig, CentralityAlgorithmResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            DegreeCentrality,
            () -> estimationFacade.degreeCentrality(configuration),
            (graph, __) -> centralityAlgorithms.degreeCentrality(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }

    public <RESULT> Stream<RESULT> eigenvector(
        GraphName graphName,
        PageRankStreamConfig configuration,
        StreamResultBuilder<PageRankStreamConfig, PageRankResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            EigenVector,
            estimationFacade::pageRank,
            (graph, __) -> centralityAlgorithms.eigenVector(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }

    public <RESULT> Stream<RESULT> harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityStreamConfig configuration,
        StreamResultBuilder<HarmonicCentralityStreamConfig, HarmonicResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            HarmonicCentrality,
            estimationFacade::harmonicCentrality,
            (graph, __) -> centralityAlgorithms.harmonicCentrality(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }

    public <RESULT> Stream<RESULT> pageRank(
        GraphName graphName,
        PageRankStreamConfig configuration,
        StreamResultBuilder<PageRankStreamConfig, PageRankResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            PageRank,
            estimationFacade::pageRank,
            (graph, __) -> centralityAlgorithms.pageRank(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.empty()
        );
    }
}
