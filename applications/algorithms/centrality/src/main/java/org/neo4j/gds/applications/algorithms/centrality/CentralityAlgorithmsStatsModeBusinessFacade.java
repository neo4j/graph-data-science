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
import org.neo4j.gds.closeness.ClosenessCentralityBaseConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentralityBaseConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationBaseConfig;
import org.neo4j.gds.pagerank.ArticleRankConfig;
import org.neo4j.gds.pagerank.EigenvectorConfig;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BetweennessCentrality;

public final class CentralityAlgorithmsStatsModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final TrackedCentralityAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final InstrumentedCentralityAlgorithms instrumentedCentralityAlgorithms;
    private final Synchroniser synchroniser;

    CentralityAlgorithmsStatsModeBusinessFacade(
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

    public <RESULT> RESULT articleRank(
        GraphName graphName,
        ArticleRankConfig configuration,
        StatsResultBuilder<PageRankResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.articleRank(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT articulationPoints(
        GraphName graphName,
        ArticulationPointsBaseConfig configuration,
        StatsResultBuilder<ArticulationPointsResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.articulationPoints(
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
        InfluenceMaximizationBaseConfig configuration,
        StatsResultBuilder<CELFResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.celf(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityBaseConfig configuration,
        StatsResultBuilder<ClosenessCentralityResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.closenessCentrality(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT degreeCentrality(
        GraphName graphName,
        DegreeCentralityConfig configuration,
        StatsResultBuilder<DegreeCentralityResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.degreeCentrality(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT eigenVector(
        GraphName graphName,
        EigenvectorConfig configuration,
        StatsResultBuilder<PageRankResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.eigenVector(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityBaseConfig configuration,
        StatsResultBuilder<HarmonicResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.harmonicCentrality(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT hits(
        GraphName graphName,
        HitsConfig configuration,
        StatsResultBuilder<PregelResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.hits(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT pageRank(
        GraphName graphName,
        PageRankConfig configuration,
        StatsResultBuilder<PageRankResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.pageRank(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }
}
