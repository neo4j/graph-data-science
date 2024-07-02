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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.degree.DegreeCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStreamConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankStreamConfig;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ArticleRank;
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

    public CentralityAlgorithmsStreamModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CentralityAlgorithms centralityAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.estimationFacade = estimationFacade;
        this.centralityAlgorithms = centralityAlgorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> RESULT articleRank(
        GraphName graphName,
        PageRankStreamConfig configuration,
        ResultBuilder<PageRankStreamConfig, PageRankResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            ArticleRank,
            estimationFacade::pageRank,
            graph -> centralityAlgorithms.articleRank(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityStreamConfig configuration,
        ResultBuilder<BetweennessCentralityStreamConfig, CentralityAlgorithmResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            BetweennessCentrality,
            () -> estimationFacade.betweennessCentrality(configuration),
            graph -> centralityAlgorithms.betweennessCentrality(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT celf(
        GraphName graphName,
        InfluenceMaximizationStreamConfig configuration,
        ResultBuilder<InfluenceMaximizationStreamConfig, CELFResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            CELF,
            () -> estimationFacade.celf(configuration),
            graph -> centralityAlgorithms.celf(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityStreamConfig configuration,
        ResultBuilder<ClosenessCentralityStreamConfig, CentralityAlgorithmResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            ClosenessCentrality,
            () -> estimationFacade.closenessCentrality(configuration),
            graph -> centralityAlgorithms.closenessCentrality(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT degreeCentrality(
        GraphName graphName,
        DegreeCentralityStreamConfig configuration,
        ResultBuilder<DegreeCentralityStreamConfig, CentralityAlgorithmResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            DegreeCentrality,
            () -> estimationFacade.degreeCentrality(configuration),
            graph -> centralityAlgorithms.degreeCentrality(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT eigenvector(
        GraphName graphName,
        PageRankStreamConfig configuration,
        ResultBuilder<PageRankStreamConfig, PageRankResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            EigenVector,
            estimationFacade::pageRank,
            graph -> centralityAlgorithms.eigenVector(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityStreamConfig configuration,
        ResultBuilder<HarmonicCentralityStreamConfig, HarmonicResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            HarmonicCentrality,
            estimationFacade::harmonicCentrality,
            graph -> centralityAlgorithms.harmonicCentrality(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT pageRank(
        GraphName graphName,
        PageRankStreamConfig configuration,
        ResultBuilder<PageRankStreamConfig, PageRankResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            PageRank,
            estimationFacade::pageRank,
            graph -> centralityAlgorithms.pageRank(graph, configuration),
            resultBuilder
        );
    }
}
