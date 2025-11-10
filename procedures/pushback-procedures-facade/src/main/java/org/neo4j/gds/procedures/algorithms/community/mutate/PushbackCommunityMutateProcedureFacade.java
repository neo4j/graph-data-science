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
import org.neo4j.gds.community.StandardCommunityProperties;
import org.neo4j.gds.config.ConsecutiveIdsConfig;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.config.SeedConfig;
import org.neo4j.gds.hdbscan.HDBScanMutateConfig;
import org.neo4j.gds.k1coloring.K1ColoringMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kmeans.KmeansMutateConfig;
import org.neo4j.gds.procedures.algorithms.community.CliqueCountingMutateResult;
import org.neo4j.gds.procedures.algorithms.community.HDBScanMutateResult;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringMutateResult;
import org.neo4j.gds.procedures.algorithms.community.KCoreDecompositionMutateResult;
import org.neo4j.gds.procedures.algorithms.community.KMeansMutateResult;
import org.neo4j.gds.procedures.algorithms.community.KmeansStatisticsComputationInstructions;
import org.neo4j.gds.procedures.algorithms.community.LocalClusteringCoefficientMutateResult;
import org.neo4j.gds.procedures.algorithms.community.SccMutateResult;
import org.neo4j.gds.procedures.algorithms.community.SpeakerListenerLPAMutateResult;
import org.neo4j.gds.procedures.algorithms.community.TriangleCountMutateResult;
import org.neo4j.gds.procedures.algorithms.community.WccMutateResult;
import org.neo4j.gds.procedures.algorithms.community.stats.ProcedureStatisticsComputationInstructions;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.scc.SccMutateConfig;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMutateConfig;
import org.neo4j.gds.triangle.TriangleCountMutateConfig;
import org.neo4j.gds.wcc.WccMutateConfig;

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

    public Stream<K1ColoringMutateResult> k1Coloring(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, K1ColoringMutateConfig::of);

        var parameters = config.toParameters();
        return businessFacade.k1Coloring(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new K1ColoringMutateResultTransformer(
                config.toMap(),
                procedureReturnColumns.contains("colorCount"),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore()
            )
        ).join();
    }

    public Stream<KCoreDecompositionMutateResult> kCore(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, KCoreDecompositionMutateConfig::of);

        return businessFacade.kCoreDecomposition(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new KCoreMutateResultTransformer(
                config.toMap(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore())
        ).join();
    }

    public Stream<KMeansMutateResult> kMeans(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, KmeansMutateConfig::of);

        var statisticsInstructions =  KmeansStatisticsComputationInstructions.create(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.kMeans(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new KMeansMutateResultTransformer(
                config.toMap(),
                statisticsInstructions,
                parameters.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore()
            )
        ).join();
    }

    public Stream<HDBScanMutateResult> hdbscan(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, HDBScanMutateConfig::of);

        return businessFacade.hdbscan(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new HDBScanMutateResultTransformer(
                config.toMap(),
                graphResources.graph().nodeCount(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore()
            )
        ).join();
    }

    public Stream<LocalClusteringCoefficientMutateResult> lcc(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LocalClusteringCoefficientMutateConfig::of);

        return businessFacade.lcc(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new LccMutateResultTransformer(
                config.toMap(),
                graphResources.graph().nodeCount(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore()
            )
        ).join();
    }

    public Stream<TriangleCountMutateResult> triangleCount(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, TriangleCountMutateConfig::of);

        return businessFacade.triangleCount(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new TriangleCountMutateResultTransformer(
                config.toMap(),
                graphResources.graph().nodeCount(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore()
            )
        ).join();
    }

    public Stream<SpeakerListenerLPAMutateResult> sllpa(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, SpeakerListenerLPAConfig::of);

        return businessFacade.sllpa(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config,
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new SpeakerListenerLPAMutateResultTransformer(
                config.toMap(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore()
            )
        ).join();
    }

    public Stream<SccMutateResult> scc(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, SccMutateConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.scc(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new SccMutateResultTransformer(
                config.toMap(),
                config.consecutiveIds(),
                statisticsInstructions,
                parameters.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore()
            )
        ).join();
    }

    public Stream<WccMutateResult> wcc(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, WccMutateConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns);

        var parameters = config.toParameters();

        var standardCommunityProperties = createNodePropertyCalculator(config);

        return businessFacade.wcc(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new WccMutateResultTransformer(
                config.toMap(),
                statisticsInstructions,
                parameters.concurrency(),
                mutateNodePropertyService,
                config.nodeLabels(),
                config.mutateProperty(),
                graphResources.graph(),
                graphResources.graphStore(),
                standardCommunityProperties
            )
        ).join();
    }


    /*
    public Stream<LabelPropagationMutateResult> labelPropagation(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LabelPropagationMutateConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.labelPropagation(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new LabelPropagationMutateResultTransformer(config.toMap(),statisticsInstructions,parameters.concurrency())
        ).join();
    }

    public Stream<LeidenMutateResult> leiden(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LeidenMutateConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns);


        var parameters = config.toParameters();
        return businessFacade.leiden(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new LeidenMutateResultTransformer(config.toMap(),statisticsInstructions,parameters.concurrency())
        ).join();
    }

    public Stream<LouvainMutateResult> louvain(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LouvainMutateConfig::of);
        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.louvain(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new LouvainMutateResultTransformer(config.toMap(),statisticsInstructions,parameters.concurrency())
        ).join();
    }

    public Stream<ModularityOptimizationMutateResult> modularityOptimization(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ModularityOptimizationMutateConfig::of);

        var statisticsInstructions =  ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns);

        var parameters = config.toParameters();
        return businessFacade.modularityOptimization(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new ModularityOptimizationMutateResultTransformer(config.toMap(),statisticsInstructions,parameters.concurrency())
        ).join();
    }
    */

    static <C extends MutateNodePropertyConfig & SeedConfig & ConsecutiveIdsConfig> StandardCommunityProperties createNodePropertyCalculator(C config) {
        return new StandardCommunityProperties(
            config.isIncremental(),
            config.seedProperty(),
            config.consecutiveIds(),
            config.mutateProperty()
        );
    }

}
