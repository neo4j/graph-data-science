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
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultRenderer;
import org.neo4j.gds.articulationpoints.ArticulationPointsBaseConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentralityStatsConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStatsConfig;
import org.neo4j.gds.degree.DegreeCentralityStatsConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityBaseConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStatsConfig;
import org.neo4j.gds.pagerank.ArticleRankStatsConfig;
import org.neo4j.gds.pagerank.EigenvectorStatsConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankStatsConfig;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticleRank;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.CELF;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ClosenessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.DegreeCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.EigenVector;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PageRank;

public final class CentralityAlgorithmsStatsModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final InstrumentedCentralityAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final CentralityAlgorithmsBusinessFacade centralityAlgorithmsBusinessFacade;
    private final Synchroniser synchroniser;

    CentralityAlgorithmsStatsModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        InstrumentedCentralityAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        CentralityAlgorithmsBusinessFacade centralityAlgorithmsBusinessFacade,
        Synchroniser synchroniser
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.centralityAlgorithmsBusinessFacade = centralityAlgorithmsBusinessFacade;
        this.synchroniser = synchroniser;
    }

    public <RESULT> RESULT articleRank(
        GraphName graphName,
        ArticleRankStatsConfig configuration,
        StatsResultBuilder<PageRankResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            ArticleRank,
            estimationFacade::pageRank,
            (graph, __) -> algorithms.articleRank(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT articulationPoints(
        GraphName graphName,
        ArticulationPointsBaseConfig configuration,
        StatsResultBuilder<ArticulationPointsResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> centralityAlgorithmsBusinessFacade.articulationPoints(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder),
            false
        ));
    }

    public <RESULT> RESULT betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityStatsConfig configuration,
        StatsResultBuilder<CentralityAlgorithmResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            BetweennessCentrality,
            () -> estimationFacade.betweennessCentrality(configuration),
            (graph, __) -> algorithms.betweennessCentrality(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT celf(
        GraphName graphName,
        InfluenceMaximizationStatsConfig configuration,
        StatsResultBuilder<CELFResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            CELF,
            () -> estimationFacade.celf(configuration),
            (graph, __) -> algorithms.celf(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityStatsConfig configuration,
        StatsResultBuilder<CentralityAlgorithmResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            ClosenessCentrality,
            estimationFacade::closenessCentrality,
            (graph, __) -> algorithms.closenessCentrality(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT degreeCentrality(
        GraphName graphName,
        DegreeCentralityStatsConfig configuration,
        StatsResultBuilder<CentralityAlgorithmResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            DegreeCentrality,
            () -> estimationFacade.degreeCentrality(configuration),
            (graph, __) -> algorithms.degreeCentrality(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT eigenVector(
        GraphName graphName,
        EigenvectorStatsConfig configuration,
        StatsResultBuilder<PageRankResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            EigenVector,
            estimationFacade::pageRank,
            (graph, __) -> algorithms.eigenVector(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityBaseConfig configuration,
        StatsResultBuilder<HarmonicResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> centralityAlgorithmsBusinessFacade.harmonicCentrality(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT pageRank(
        GraphName graphName,
        PageRankStatsConfig configuration,
        StatsResultBuilder<PageRankResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            PageRank,
            estimationFacade::pageRank,
            (graph, __) -> algorithms.pageRank(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT hits(
        GraphName graphName,
        HitsConfig configuration,
        StatsResultBuilder<PregelResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> centralityAlgorithmsBusinessFacade.hits(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }
}
