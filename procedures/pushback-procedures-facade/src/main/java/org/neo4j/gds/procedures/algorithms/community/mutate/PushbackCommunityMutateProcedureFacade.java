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
package org.neo4j.gds.procedures.algorithms.community.mutate;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.cliquecounting.CliqueCountingMutateConfig;
import org.neo4j.gds.community.CommunityComputeBusinessFacade;
import org.neo4j.gds.procedures.algorithms.community.CliqueCountingMutateResult;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackCommunityMutateProcedureFacade {

    private final CommunityComputeBusinessFacade businessFacade;
    private final UserSpecificConfigurationParser configurationParser;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final MutateNodePropertyService mutateNodePropertyService;


    public PushbackCommunityMutateProcedureFacade(
        CommunityComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        ProcedureReturnColumns procedureReturnColumns,
        MutateNodePropertyService mutateNodePropertyService
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.procedureReturnColumns = procedureReturnColumns;
        this.mutateNodePropertyService = mutateNodePropertyService;
    }

    public Stream<CliqueCountingMutateResult> cliqueCounting(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, CliqueCountingMutateConfig::of);

        var parameters = config.toParameters();
        return businessFacade.cliqueCounting(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CliqueCountingMutateResultTransformer(
                config.toMap(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore()
            )
        ).join();
    }
    /*
    public Stream<K1ColoringStatsResult> k1Coloring(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, K1ColoringStatsConfig::of);

        var parameters = config.toParameters();
        return businessFacade.k1Coloring(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new K1ColoringStatsResultTransformer(config.toMap(), procedureReturnColumns.contains("colorCount"))
        ).join();
    }

    public Stream<KCoreDecompositionStatsResult> kCore(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, KCoreDecompositionStatsConfig::of);

        return businessFacade.kCoreDecomposition(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new KCoreStatsResultTransformer(config.toMap())
        ).join();
    }

    public Stream<KmeansStatsResult> kMeans(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, KmeansStatsConfig::of);

        var statisticsInstructions =  KmeansStatisticsComputationInstructions.create(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.kMeans(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new KMeansStatsResultTransformer(config.toMap(), statisticsInstructions, parameters.concurrency())
        ).join();
    }

    public Stream<LabelPropagationStatsResult> labelPropagation(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LabelPropagationStatsConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.labelPropagation(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new LabelPropagationStatsResultTransformer(config.toMap(),statisticsInstructions,parameters.concurrency())
        ).join();
    }

    public Stream<LeidenStatsResult> leiden(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LeidenStatsConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns);


        var parameters = config.toParameters();
        return businessFacade.leiden(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new LeidenStatsResultTransformer(config.toMap(),statisticsInstructions,parameters.concurrency())
        ).join();
    }

    public Stream<LouvainStatsResult> louvain(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LouvainStatsConfig::of);
        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.louvain(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new LouvainStatsResultTransformer(config.toMap(),statisticsInstructions,parameters.concurrency())
        ).join();
    }

    public Stream<ModularityOptimizationStatsResult> modularityOptimization(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ModularityOptimizationStatsConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.modularityOptimization(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new ModularityOptimizationStatsResultTransformer(config.toMap(),statisticsInstructions,parameters.concurrency())
        ).join();
    }



     public Stream<HDBScanStatsResult> hdbscan(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, HDBScanStatsConfig::of);

        return businessFacade.hdbscan(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new HDBScanStatsResultTransformer(config.toMap(),graphResources.graph().nodeCount())
        ).join();
    }

    public Stream<ModularityStatsResult> modularity(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ModularityStatsConfig::of);

        return businessFacade.modularity(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            graphResources -> new ModularityStatsResultTransformer(config.toMap())
        ).join();
    }


    public Stream<LocalClusteringCoefficientStatsResult> lcc(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LocalClusteringCoefficientStatsConfig::of);

        return businessFacade.lcc(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new LccStatsResultTransformer(config.toMap(),graphResources.graph().nodeCount())
        ).join();
    }

    public Stream<TriangleCountStatsResult> triangleCount(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, TriangleCountStatsConfig::of);

        return businessFacade.triangleCount(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new TriangleCountStatsResultTransformer(config.toMap(),graphResources.graph().nodeCount())
        ).join();
    }


    public Stream<SccStatsResult> scc(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, SccStatsConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.scc(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new SccStatsResultTransformer(config.toMap(),statisticsInstructions, parameters.concurrency())
        ).join();
    }

    public Stream<WccStatsResult> wcc(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, WccStatsConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.wcc(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new WccStatsResultTransformer(config.toMap(),statisticsInstructions, parameters.concurrency())
        ).join();
    }


    public Stream<SpeakerListenerLPAStatsResult> sllpa(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, SpeakerListenerLPAConfig::of);

        return businessFacade.sllpa(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config,
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new SpeakerListenerLPAStatsResultTransformer(config.toMap())
        ).join();
    }

    */



}
