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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.betweenness.BetweennessCentralityStatsConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStatsConfig;
import org.neo4j.gds.degree.DegreeCentralityStatsConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStatsConfig;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStatsConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankStatsConfig;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ArticleRank;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.CELF;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ClosenessCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.DegreeCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.EigenVector;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.HarmonicCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.PageRank;

public class CentralityAlgorithmsStatsModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CentralityAlgorithms centralityAlgorithms;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;

    public CentralityAlgorithmsStatsModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CentralityAlgorithms centralityAlgorithms,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        this.estimationFacade = estimationFacade;
        this.centralityAlgorithms = centralityAlgorithms;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
    }

    public <RESULT> RESULT articleRank(
        GraphName graphName,
        PageRankStatsConfig configuration,
        ResultBuilder<PageRankStatsConfig, PageRankResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            ArticleRank,
            estimationFacade::pageRank,
            graph -> centralityAlgorithms.articleRank(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityStatsConfig configuration,
        ResultBuilder<BetweennessCentralityStatsConfig, CentralityAlgorithmResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            BetweennessCentrality,
            () -> estimationFacade.betweennessCentrality(configuration),
            graph -> centralityAlgorithms.betweennessCentrality(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT celf(
        GraphName graphName,
        InfluenceMaximizationStatsConfig configuration,
        ResultBuilder<InfluenceMaximizationStatsConfig, CELFResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            CELF,
            () -> estimationFacade.celf(configuration),
            graph -> centralityAlgorithms.celf(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityStatsConfig configuration,
        ResultBuilder<ClosenessCentralityStatsConfig, CentralityAlgorithmResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            ClosenessCentrality,
            () -> estimationFacade.closenessCentrality(configuration),
            graph -> centralityAlgorithms.closenessCentrality(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT degreeCentrality(
        GraphName graphName,
        DegreeCentralityStatsConfig configuration,
        ResultBuilder<DegreeCentralityStatsConfig, CentralityAlgorithmResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            DegreeCentrality,
            () -> estimationFacade.degreeCentrality(configuration),
            graph -> centralityAlgorithms.degreeCentrality(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT eigenVector(
        GraphName graphName,
        PageRankStatsConfig configuration,
        ResultBuilder<PageRankStatsConfig, PageRankResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            EigenVector,
            estimationFacade::pageRank,
            graph -> centralityAlgorithms.eigenVector(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityStatsConfig configuration,
        ResultBuilder<HarmonicCentralityStatsConfig, CentralityAlgorithmResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            HarmonicCentrality,
            estimationFacade::harmonicCentrality,
            graph -> centralityAlgorithms.harmonicCentrality(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT pageRank(
        GraphName graphName,
        PageRankStatsConfig configuration,
        ResultBuilder<PageRankStatsConfig, PageRankResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            PageRank,
            estimationFacade::pageRank,
            graph -> centralityAlgorithms.pageRank(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }
}
