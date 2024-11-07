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
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamConfig;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.BridgesStreamConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.degree.DegreeCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStreamConfig;
import org.neo4j.gds.pagerank.ArticleRankStreamConfig;
import org.neo4j.gds.pagerank.EigenvectorStreamConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankStreamConfig;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticleRank;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticulationPoints;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Bridges;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.CELF;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ClosenessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.DegreeCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.EigenVector;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.HITS;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.HarmonicCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PageRank;

public class CentralityAlgorithmsStreamModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CentralityAlgorithms centralityAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final HitsHookGenerator hitsHookGenerator;

    CentralityAlgorithmsStreamModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CentralityAlgorithms centralityAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        HitsHookGenerator hitsHookGenerator
    ) {
        this.estimationFacade = estimationFacade;
        this.centralityAlgorithms = centralityAlgorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.hitsHookGenerator = hitsHookGenerator;
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
            (graph, __) -> centralityAlgorithms.articleRank(graph, configuration),
            streamResultBuilder
        );
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
            (graph, __) -> centralityAlgorithms.betweennessCentrality(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> articulationPoints(
        GraphName graphName,
        ArticulationPointsStreamConfig configuration,
        StreamResultBuilder<BitSet, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ArticulationPoints,
            estimationFacade::articulationPoints,
            (graph, __) -> centralityAlgorithms.articulationPoints(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> bridges(
        GraphName graphName,
        BridgesStreamConfig configuration,
        StreamResultBuilder<BridgeResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Bridges,
            estimationFacade::bridges,
            (graph, __) -> centralityAlgorithms.bridges(graph, configuration),
            streamResultBuilder
        );
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
            (graph, __) -> centralityAlgorithms.celf(graph, configuration),
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
            () -> estimationFacade.closenessCentrality(configuration),
            (graph, __) -> centralityAlgorithms.closenessCentrality(graph, configuration),
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
            (graph, __) -> centralityAlgorithms.degreeCentrality(graph, configuration),
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
            (graph, __) -> centralityAlgorithms.eigenVector(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityStreamConfig configuration,
        StreamResultBuilder<HarmonicResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            HarmonicCentrality,
            estimationFacade::harmonicCentrality,
            (graph, __) -> centralityAlgorithms.harmonicCentrality(graph, configuration),
            streamResultBuilder
        );
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
            (graph, __) -> centralityAlgorithms.pageRank(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> hits(
        GraphName graphName,
        HitsConfig configuration,
        StreamResultBuilder<PregelResult, RESULT> streamResultBuilder
    ) {

        var hitsETLHook = hitsHookGenerator.createETLHook(configuration);

        return algorithmProcessingTemplateConvenience.processAlgorithmInStreamMode(
            graphName,
            configuration,
            HITS,
            estimationFacade::hits,
            (graph, __) -> centralityAlgorithms.hits(graph, configuration),
            streamResultBuilder,
            Optional.empty(),
            Optional.of(List.of(hitsETLHook)),
            Optional.empty()
        );
    }

}
