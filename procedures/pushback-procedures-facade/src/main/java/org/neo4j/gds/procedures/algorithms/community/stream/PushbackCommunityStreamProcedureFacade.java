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
package org.neo4j.gds.procedures.algorithms.community.stream;

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutStreamConfig;
import org.neo4j.gds.cliquecounting.CliqueCountingStreamConfig;
import org.neo4j.gds.community.CommunityComputeBusinessFacade;
import org.neo4j.gds.conductance.ConductanceConfigTransformer;
import org.neo4j.gds.conductance.ConductanceStreamConfig;
import org.neo4j.gds.hdbscan.HDBScanStreamConfig;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfig;
import org.neo4j.gds.kmeans.KmeansStreamConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfig;
import org.neo4j.gds.leiden.LeidenStreamConfig;
import org.neo4j.gds.louvain.LouvainStreamConfig;
import org.neo4j.gds.modularity.ModularityStreamConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStreamConfig;
import org.neo4j.gds.procedures.algorithms.community.ApproxMaxKCutStreamResult;
import org.neo4j.gds.procedures.algorithms.community.CliqueCountingStreamResult;
import org.neo4j.gds.procedures.algorithms.community.ConductanceStreamResult;
import org.neo4j.gds.procedures.algorithms.community.HDBScanStreamResult;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringStreamResult;
import org.neo4j.gds.procedures.algorithms.community.KCoreDecompositionStreamResult;
import org.neo4j.gds.procedures.algorithms.community.KMeansStreamResult;
import org.neo4j.gds.procedures.algorithms.community.LabelPropagationStreamResult;
import org.neo4j.gds.procedures.algorithms.community.LeidenStreamResult;
import org.neo4j.gds.procedures.algorithms.community.LocalClusteringCoefficientStreamResult;
import org.neo4j.gds.procedures.algorithms.community.LouvainStreamResult;
import org.neo4j.gds.procedures.algorithms.community.ModularityOptimizationStreamResult;
import org.neo4j.gds.procedures.algorithms.community.ModularityStreamResult;
import org.neo4j.gds.procedures.algorithms.community.SccStreamResult;
import org.neo4j.gds.procedures.algorithms.community.TriangleCountStreamResult;
import org.neo4j.gds.procedures.algorithms.community.TriangleStreamResult;
import org.neo4j.gds.procedures.algorithms.community.WccStreamResult;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.scc.SccStreamConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStreamConfig;
import org.neo4j.gds.triangle.TriangleCountStreamConfig;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackCommunityStreamProcedureFacade {

    private final CommunityComputeBusinessFacade businessFacade;
    private final UserSpecificConfigurationParser configurationParser;
    private final CloseableResourceRegistry closeableResourceRegistry;

    public PushbackCommunityStreamProcedureFacade(
        CommunityComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        CloseableResourceRegistry closeableResourceRegistry
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.closeableResourceRegistry = closeableResourceRegistry;
    }

    public Stream<ApproxMaxKCutStreamResult> approxMaxKCut(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ApproxMaxKCutStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.approxMaxKCut(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new ApproxMaxKCutStreamTransformer(graphResources.graph(), parameters.concurrency(),config.minCommunitySize())
        ).join();
    }

    public Stream<CliqueCountingStreamResult> cliqueCounting(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, CliqueCountingStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.cliqueCounting(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CliqueCountingStreamTransformer(graphResources.graph())
        ).join();
    }

    public Stream<ConductanceStreamResult> conductance(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ConductanceStreamConfig::of);

        return businessFacade.conductance(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            ConductanceConfigTransformer.toParameters(config),
            config.jobId(),
            config.logProgress(),
            graphResources -> new ConductanceStreamTransformer()
        ).join();
    }

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

    public Stream<K1ColoringStreamResult> k1Coloring(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, K1ColoringStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.k1Coloring(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new K1ColoringCutStreamTransformer(graphResources.graph(),parameters.concurrency(),config.minCommunitySize())
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

    public Stream<TriangleStreamResult> triangles(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, TriangleCountStreamConfig::of);

        return businessFacade.triangles(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            (graphResources)-> new TrianglesStreamTransformer(closeableResourceRegistry)
        ).join();
    }

}
