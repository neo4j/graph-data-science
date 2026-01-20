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
package org.neo4j.gds.procedures.algorithms.centrality.mutate;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.articulationpoints.ArticulationPointsMutateConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsToParameters;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.centrality.CentralityComputeBusinessFacade;
import org.neo4j.gds.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.degree.DegreeCentralityMutateConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityMutateConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationMutateConfig;
import org.neo4j.gds.pagerank.ArticleRankMutateConfig;
import org.neo4j.gds.pagerank.EigenvectorMutateConfig;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.procedures.algorithms.CentralityDistributionInstructions;
import org.neo4j.gds.procedures.algorithms.centrality.ArticulationPointsMutateResult;
import org.neo4j.gds.procedures.algorithms.centrality.BetaClosenessCentralityMutateResult;
import org.neo4j.gds.procedures.algorithms.centrality.CELFMutateResult;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityMutateResult;
import org.neo4j.gds.procedures.algorithms.centrality.PageRankMutateResult;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackCentralityMutateProcedureFacade {

    private final CentralityComputeBusinessFacade businessFacade;
    private final UserSpecificConfigurationParser configurationParser;
    private final CentralityDistributionInstructions centralityDistributionInstructions;
    private final MutateNodePropertyService mutateNodePropertyService;

    public PushbackCentralityMutateProcedureFacade(
        CentralityComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        ProcedureReturnColumns procedureReturnColumns, MutateNodePropertyService mutateNodePropertyService
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.centralityDistributionInstructions = new CentralityDistributionInstructions(procedureReturnColumns);
        this.mutateNodePropertyService = mutateNodePropertyService;
    }

    public Stream<PageRankMutateResult> articleRank(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ArticleRankMutateConfig::of);
        var scalerFactory = config.scaler();
        return businessFacade.articleRank(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config,
            config.jobId(),
            config.logProgress(),
            graphResources -> new GenericRankMutateResultTransformer(
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap(),
                scalerFactory,
                centralityDistributionInstructions.shouldComputeDistribution(),
                config.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty()
            )
        ).join();
    }

    public Stream<BetaClosenessCentralityMutateResult> betaCloseness(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ClosenessCentralityMutateConfig::of);

        var parameters = config.toParameters();
        return businessFacade.closeness(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new BetaClosenessCentralityMutateResultTransformer(
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap(),
                centralityDistributionInstructions.shouldComputeDistribution(),
                parameters.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty()
            )
        ).join();
    }

    public Stream<CentralityMutateResult> betweenness(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, BetweennessCentralityMutateConfig::of);

        var parameters = config.toParameters();
        return businessFacade.betweennessCentrality(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.relationshipWeightProperty(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new GenericCentralityMutateResultTransformer<>(
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap(),
                centralityDistributionInstructions.shouldComputeDistribution(),
                parameters.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty()
            )
        ).join();
    }

    public Stream<CentralityMutateResult> closeness(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ClosenessCentralityMutateConfig::of);

        var parameters = config.toParameters();
        return businessFacade.closeness(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new GenericCentralityMutateResultTransformer<>(
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap(),
                centralityDistributionInstructions.shouldComputeDistribution(),
                parameters.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty()
            )
        ).join();
    }


    public Stream<ArticulationPointsMutateResult> articulationPoints(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(configuration, ArticulationPointsMutateConfig::of);

        var parameters = ArticulationPointsToParameters.toParameters(config,false);
        return businessFacade.articulationPoints(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new ArticulationPointsMutateResultTransformer
                (graphResources.graph(),
                    graphResources.graphStore(),
                    config.toMap(),
                    mutateNodePropertyService,
                    config.nodeLabels(),
                    config.mutateProperty()
                )
        ).join();
    }

    public Stream<CELFMutateResult> celf(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, InfluenceMaximizationMutateConfig::of);

        var parameters = config.toParameters();
        return businessFacade.celf(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CelfMutateResultTransformer(
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty()
            )
        ).join();
    }

    public Stream<CentralityMutateResult> degree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, DegreeCentralityMutateConfig::of);

        var parameters = config.toParameters();
        return businessFacade.degree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new GenericCentralityMutateResultTransformer<>(
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap(),
                centralityDistributionInstructions.shouldComputeDistribution(),
                parameters.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty()
            )
        ).join();
    }


    public Stream<PageRankMutateResult> eigenVector(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, EigenvectorMutateConfig::of);
        var scalerFactory = config.scaler();
        return businessFacade.eigenVector(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config,
            config.jobId(),
            config.logProgress(),
            graphResources -> new GenericRankMutateResultTransformer(
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap(),
                scalerFactory,
                centralityDistributionInstructions.shouldComputeDistribution(),
                config.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty()
            )
        ).join();
    }


    public Stream<CentralityMutateResult> harmonic(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, HarmonicCentralityMutateConfig::of);

        var parameters = config.toParameters();
        return businessFacade.harmonic(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new GenericCentralityMutateResultTransformer<>(
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap(),
                centralityDistributionInstructions.shouldComputeDistribution(),
                parameters.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty()
            )
        ).join();
    }

    public Stream<PageRankMutateResult> pageRank(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, PageRankMutateConfig::of);
        var scalerFactory = config.scaler();
        return businessFacade.pageRank(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config,
            config.jobId(),
            config.logProgress(),
            graphResources -> new GenericRankMutateResultTransformer(
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap(),
                scalerFactory,
                centralityDistributionInstructions.shouldComputeDistribution(),
                config.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty()
            )
        ).join();
    }

}
