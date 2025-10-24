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
package org.neo4j.gds.procedures.algorithms.community.stats;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.cliquecounting.CliqueCountingStatsConfig;
import org.neo4j.gds.community.CommunityComputeBusinessFacade;
import org.neo4j.gds.k1coloring.K1ColoringStatsConfig;
import org.neo4j.gds.procedures.algorithms.community.CliqueCountingStatsResult;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringStatsResult;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackCommunityStatsProcedureFacade {

    private final CommunityComputeBusinessFacade businessFacade;
    private final UserSpecificConfigurationParser configurationParser;
    private final ProcedureReturnColumns procedureReturnColumns;


    public PushbackCommunityStatsProcedureFacade(
        CommunityComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        ProcedureReturnColumns procedureReturnColumns
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.procedureReturnColumns = procedureReturnColumns;
    }

    public Stream<CliqueCountingStatsResult> cliqueCounting(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, CliqueCountingStatsConfig::of);

        var parameters = config.toParameters();
        return businessFacade.cliqueCounting(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CliqueCountingStatsResultTransformer(config.toMap())
        ).join();
    }

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
    /*

     public Stream<HDBScanStreamResult> hdbscan(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, HDBScanStreamConfig::of);

        return businessFacade.hdbscan(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new HDBScanStreamTransformer(graphResources.graph())
        ).join();
    }


    public Stream<KCoreDecompositionStreamResult> kCore(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, KCoreDecompositionStreamConfig::of);

        return businessFacade.kCoreDecomposition(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new KCoreStreamTransformer(graphResources.graph())
        ).join();
    }

    public Stream<KMeansStreamResult> kMeans(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, KmeansStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.kMeans(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new KMeansStreamTransformer(graphResources.graph())
        ).join();
    }

    public Stream<LabelPropagationStreamResult> labelPropagation(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LabelPropagationStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.labelPropagation(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new LabelPropagationStreamTransformer(graphResources.graph(), parameters.concurrency(),config.minCommunitySize(),config.consecutiveIds())
        ).join();
    }

    public Stream<LocalClusteringCoefficientStreamResult> lcc(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LocalClusteringCoefficientStreamConfig::of);

        return businessFacade.lcc(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new LccStreamTransformer(graphResources.graph())
        ).join();
    }

    public Stream<LeidenStreamResult> leiden(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LeidenStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.leiden(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new LeidenStreamTransformer(graphResources.graph(), parameters.concurrency(),config.minCommunitySize(),config.consecutiveIds(),config.includeIntermediateCommunities())
        ).join();
    }

    public Stream<LouvainStreamResult> louvain(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, LouvainStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.louvain(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new LouvainStreamTransformer(graphResources.graph(), parameters.concurrency(),config.minCommunitySize(),config.consecutiveIds(),config.includeIntermediateCommunities())
        ).join();
    }

    public Stream<ModularityOptimizationStreamResult> modularityOptimization(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ModularityOptimizationStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.modularityOptimization(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new ModularityOptimizationStreamTransformer(graphResources.graph(), parameters.concurrency(),config.minCommunitySize(),config.consecutiveIds())
        ).join();
    }

    public Stream<ModularityStreamResult> modularity(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ModularityStreamConfig::of);

        return businessFacade.modularity(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            graphResources -> new ModularityStreamTransformer()
        ).join();
    }

    public Stream<SccStreamResult> scc(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, SccStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.scc(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new SccStreamTransformer(graphResources.graph(), parameters.concurrency(),config.consecutiveIds())
        ).join();
    }

    public Stream<WccStreamResult> wcc(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, WccStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.wcc(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new WccStreamTransformer(graphResources.graph(), parameters.concurrency(),config.minCommunitySize(),config.consecutiveIds())
        ).join();
    }

    public Stream<TriangleCountStreamResult> triangleCount(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, TriangleCountStreamConfig::of);

        return businessFacade.triangleCount(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new TriangleCountStreamTransformer(graphResources.graph())
        ).join();
    }

    public Stream<SpeakerListenerLPAStreamResult> sllpa(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, SpeakerListenerLPAConfig::of);

        return businessFacade.sllpa(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config,
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new SpeakerListenerLPAStreamTransformer(graphResources.graph())
        ).join();
    }

     */

}
