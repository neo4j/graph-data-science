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
package org.neo4j.gds.procedures.algorithms.centrality.stats;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.articulationpoints.ArticulationPointsStatsConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsToParameters;
import org.neo4j.gds.betweenness.BetweennessCentralityStatsConfig;
import org.neo4j.gds.centrality.CentralityComputeBusinessFacade;
import org.neo4j.gds.closeness.ClosenessCentralityStatsConfig;
import org.neo4j.gds.degree.DegreeCentralityStatsConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStatsConfig;
import org.neo4j.gds.pagerank.ArticleRankStatsConfig;
import org.neo4j.gds.procedures.algorithms.CentralityDistributionInstructions;
import org.neo4j.gds.procedures.algorithms.centrality.ArticulationPointsStatsResult;
import org.neo4j.gds.procedures.algorithms.centrality.CELFStatsResult;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityStatsResult;
import org.neo4j.gds.procedures.algorithms.centrality.PageRankStatsResult;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackCentralityStatsProcedureFacade {

    private final CentralityComputeBusinessFacade businessFacade;
    private final UserSpecificConfigurationParser configurationParser;
    private final CentralityDistributionInstructions centralityDistributionInstructions;

    public PushbackCentralityStatsProcedureFacade(
        CentralityComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        ProcedureReturnColumns procedureReturnColumns
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.centralityDistributionInstructions = new CentralityDistributionInstructions(procedureReturnColumns);
    }

    public Stream<PageRankStatsResult> articleRank(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ArticleRankStatsConfig::of);
        var scalerFactory = config.scaler();
        return businessFacade.articleRank(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config,
            config.jobId(),
            config.logProgress(),
            graphResources -> new GenericRankStatsResultTransformer(
                graphResources.graph(),
                config.toMap(),
                scalerFactory,
                centralityDistributionInstructions.shouldComputeDistribution(),
                config.concurrency()
            )
        ).join();
    }

    public Stream<ArticulationPointsStatsResult> articulationPoints(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(configuration, ArticulationPointsStatsConfig::of);

        var parameters = ArticulationPointsToParameters.toParameters(config,false);
        return businessFacade.articulationPoints(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            __ -> new ArticulationPointsStatsResultTransformer(config.toMap())
        ).join();
    }

    public Stream<CentralityStatsResult> betweenness(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, BetweennessCentralityStatsConfig::of);

        var parameters = config.toParameters();
        return businessFacade.betweennessCentrality(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.relationshipWeightProperty(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new CentralityStatsResultTransformer<>(
                graphResources.graph(),
                config.toMap(),
                centralityDistributionInstructions.shouldComputeDistribution(),
                parameters.concurrency())
        ).join();
    }

    public Stream<CELFStatsResult> celf(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, InfluenceMaximizationStatsConfig::of);

        var parameters = config.toParameters();
        return businessFacade.celf(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CelfStatsResultTransformer(
                graphResources.graph().nodeCount(),
                config.toMap()
            )
        ).join();
    }

    public Stream<CentralityStatsResult> closeness(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ClosenessCentralityStatsConfig::of);

        var parameters = config.toParameters();
        return businessFacade.closeness(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CentralityStatsResultTransformer<>(
                graphResources.graph(),
                config.toMap(),
                centralityDistributionInstructions.shouldComputeDistribution(),
                parameters.concurrency()
            )
        ).join();
    }

    public Stream<CentralityStatsResult> degree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, DegreeCentralityStatsConfig::of);

        var parameters = config.toParameters();
        return businessFacade.degree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CentralityStatsResultTransformer<>(
                graphResources.graph(),
                config.toMap(),
                centralityDistributionInstructions.shouldComputeDistribution(),
                parameters.concurrency()
            )
        ).join();
    }

}
