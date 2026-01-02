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
package org.neo4j.gds.procedures.algorithms.centrality.stream;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.articulationpoints.ArticulationPointsStreamConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsToParameters;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamConfig;
import org.neo4j.gds.bridges.BridgesStreamConfig;
import org.neo4j.gds.bridges.BridgesToParameters;
import org.neo4j.gds.centrality.CentralityComputeBusinessFacade;
import org.neo4j.gds.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.degree.DegreeCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStreamConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStreamConfig;
import org.neo4j.gds.pagerank.ArticleRankStreamConfig;
import org.neo4j.gds.pagerank.EigenvectorStreamConfig;
import org.neo4j.gds.procedures.algorithms.centrality.AlphaHarmonicStreamResult;
import org.neo4j.gds.procedures.algorithms.centrality.ArticulationPointStreamResult;
import org.neo4j.gds.procedures.algorithms.centrality.BridgesStreamResult;
import org.neo4j.gds.procedures.algorithms.centrality.CELFStreamResult;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityStreamResult;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackCentralityStreamProcedureFacade {

    private final CentralityComputeBusinessFacade businessFacade;
    private final UserSpecificConfigurationParser configurationParser;
    private final ProcedureReturnColumns procedureReturnColumns;


    public PushbackCentralityStreamProcedureFacade(
        CentralityComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        ProcedureReturnColumns procedureReturnColumns
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.procedureReturnColumns = procedureReturnColumns;
    }

    public Stream<AlphaHarmonicStreamResult> alphaHarmonic(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, HarmonicCentralityStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.harmonic(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new AlphaHarmonicStreamResultTransformer(graphResources.graph())
        ).join();
    }

    public Stream<CentralityStreamResult> articleRank(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ArticleRankStreamConfig::of);

        return businessFacade.articleRank(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CentralityResultStreamTransformer<>(graphResources.graph())
        ).join();
    }

    public Stream<ArticulationPointStreamResult> articulationPoints(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(configuration, ArticulationPointsStreamConfig::of);
        var shouldComputeComponents = procedureReturnColumns.contains("resultingComponents");

        var parameters = ArticulationPointsToParameters.toParameters(config,shouldComputeComponents);
        return businessFacade.articulationPoints(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new ArticulationPointsStreamResultTransformer(graphResources.graph())
        ).join();
    }

    public Stream<CentralityStreamResult> betweenness(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, BetweennessCentralityStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.betweennessCentrality(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.relationshipWeightProperty(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new CentralityResultStreamTransformer<>(graphResources.graph())
        ).join();
    }

    public Stream<BridgesStreamResult> bridges(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(configuration, BridgesStreamConfig::of);

        var shouldComputeComponents = procedureReturnColumns.contains("remainingSizes");

        var parameters = BridgesToParameters.toParameters(config,shouldComputeComponents);
        return businessFacade.bridges(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new BridgesStreamResultTransformer(graphResources.graph(),shouldComputeComponents)
        ).join();
    }

    public Stream<CELFStreamResult> celf(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, InfluenceMaximizationStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.celf(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CelfStreamResultTransformer(graphResources.graph())
        ).join();
    }

    public Stream<CentralityStreamResult> closeness(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ClosenessCentralityStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.closeness(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CentralityResultStreamTransformer<>(graphResources.graph())
        ).join();
    }

    public Stream<CentralityStreamResult> degree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, DegreeCentralityStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.degree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CentralityResultStreamTransformer<>(graphResources.graph())
        ).join();
    }

    public Stream<CentralityStreamResult> eigenVector(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, EigenvectorStreamConfig::of);

        return businessFacade.eigenVector(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CentralityResultStreamTransformer<>(graphResources.graph())
        ).join();
    }


}
