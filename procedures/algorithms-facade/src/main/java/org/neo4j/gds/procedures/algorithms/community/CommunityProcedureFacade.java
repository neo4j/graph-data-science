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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutStreamConfig;
import org.neo4j.gds.conductance.ConductanceStreamConfig;
import org.neo4j.gds.k1coloring.K1ColoringStatsConfig;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.k1coloring.K1ColoringWriteConfig;
import org.neo4j.gds.kcore.KCoreDecompositionStatsConfig;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfig;
import org.neo4j.gds.kcore.KCoreDecompositionWriteConfig;
import org.neo4j.gds.kmeans.KmeansStatsConfig;
import org.neo4j.gds.kmeans.KmeansStreamConfig;
import org.neo4j.gds.kmeans.KmeansWriteConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationStatsConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationWriteConfig;
import org.neo4j.gds.leiden.LeidenStatsConfig;
import org.neo4j.gds.leiden.LeidenStreamConfig;
import org.neo4j.gds.leiden.LeidenWriteConfig;
import org.neo4j.gds.louvain.LouvainStatsConfig;
import org.neo4j.gds.louvain.LouvainStreamConfig;
import org.neo4j.gds.louvain.LouvainWriteConfig;
import org.neo4j.gds.modularity.ModularityStatsConfig;
import org.neo4j.gds.modularity.ModularityStreamConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStatsConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStreamConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationWriteConfig;
import org.neo4j.gds.procedures.algorithms.community.stubs.ApproximateMaximumKCutMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.K1ColoringMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.KCoreMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.KMeansMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LabelPropagationMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LccMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LeidenMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LouvainMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.ModularityOptimizationMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.SccMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.TriangleCountMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.WccMutateStub;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.scc.SccAlphaWriteConfig;
import org.neo4j.gds.scc.SccStatsConfig;
import org.neo4j.gds.scc.SccStreamConfig;
import org.neo4j.gds.scc.SccWriteConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStatsConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStreamConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientWriteConfig;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountStatsConfig;
import org.neo4j.gds.triangle.TriangleCountStreamConfig;
import org.neo4j.gds.triangle.TriangleCountWriteConfig;
import org.neo4j.gds.triangle.TriangleStreamResult;
import org.neo4j.gds.wcc.WccStatsConfig;
import org.neo4j.gds.wcc.WccStreamConfig;
import org.neo4j.gds.wcc.WccWriteConfig;

import java.util.Map;
import java.util.stream.Stream;

public final class CommunityProcedureFacade {
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final ProcedureReturnColumns procedureReturnColumns;


    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final CommunityAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade;
    private final CommunityAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade;
    private final CommunityAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade;


    private final ApproximateMaximumKCutMutateStub approximateMaximumKCutMutateStub;
    private final K1ColoringMutateStub k1ColoringMutateStub;
    private final KCoreMutateStub kCoreMutateStub;
    private final KMeansMutateStub kMeansMutateStub;
    private final LabelPropagationMutateStub labelPropagationMutateStub;
    private final LccMutateStub lccMutateStub;
    private final LeidenMutateStub leidenMutateStub;
    private final LouvainMutateStub louvainMutateStub;
    private final ModularityOptimizationMutateStub modularityOptimizationMutateStub;
    private final SccMutateStub sccMutateStub;
    private final TriangleCountMutateStub triangleCountMutateStub;
    private final WccMutateStub wccMutateStub;

    private final UserSpecificConfigurationParser configurationParser;


    private CommunityProcedureFacade(
        CloseableResourceRegistry closeableResourceRegistry,
        ProcedureReturnColumns procedureReturnColumns,
        CommunityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        CommunityAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade,
        CommunityAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade,
        CommunityAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade,
        ApproximateMaximumKCutMutateStub approximateMaximumKCutMutateStub,
        K1ColoringMutateStub k1ColoringMutateStub,
        KCoreMutateStub kCoreMutateStub,
        KMeansMutateStub kMeansMutateStub,
        LabelPropagationMutateStub labelPropagationMutateStub,
        LccMutateStub lccMutateStub,
        LeidenMutateStub leidenMutateStub,
        LouvainMutateStub louvainMutateStub,
        ModularityOptimizationMutateStub modularityOptimizationMutateStub,
        SccMutateStub sccMutateStub,
        TriangleCountMutateStub triangleCountMutateStub,
        WccMutateStub wccMutateStub,
        UserSpecificConfigurationParser configurationParser
    ) {
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.procedureReturnColumns = procedureReturnColumns;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.statsModeBusinessFacade = statsModeBusinessFacade;
        this.streamModeBusinessFacade = streamModeBusinessFacade;
        this.writeModeBusinessFacade = writeModeBusinessFacade;
        this.approximateMaximumKCutMutateStub = approximateMaximumKCutMutateStub;
        this.k1ColoringMutateStub = k1ColoringMutateStub;
        this.kCoreMutateStub = kCoreMutateStub;
        this.kMeansMutateStub = kMeansMutateStub;
        this.labelPropagationMutateStub = labelPropagationMutateStub;
        this.leidenMutateStub = leidenMutateStub;
        this.louvainMutateStub = louvainMutateStub;
        this.modularityOptimizationMutateStub = modularityOptimizationMutateStub;
        this.sccMutateStub = sccMutateStub;
        this.lccMutateStub = lccMutateStub;
        this.triangleCountMutateStub = triangleCountMutateStub;
        this.wccMutateStub = wccMutateStub;
        this.configurationParser = configurationParser;
    }

    public static CommunityProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        CloseableResourceRegistry closeableResourceRegistry,
        ProcedureReturnColumns procedureReturnColumns,
        UserSpecificConfigurationParser configurationParser
    ) {

        var approximateMaximumKCutMutateStub = new ApproximateMaximumKCutMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate()
        );

        var k1ColoringMutateStub = new K1ColoringMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate(),
            procedureReturnColumns
        );

        var kCoreMutateStub = new KCoreMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate()
        );

        var kMeansMutateStub = new KMeansMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate(),
            procedureReturnColumns
        );

        var labelPropagationMutateStub = new LabelPropagationMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate(),
            procedureReturnColumns
        );
        var lccMutateStub = new LccMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate()
        );

        var leidenMutateStub = new LeidenMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate(),
            procedureReturnColumns
        );

        var louvainMutateStub = new LouvainMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate(),
            procedureReturnColumns
        );

        var modularityOptimizationMutateStub = new ModularityOptimizationMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate(),
            procedureReturnColumns
        );

        var sccMutateStub = new SccMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate(),
            procedureReturnColumns
        );

        var triangleCountMutateStub = new TriangleCountMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate()
        );

        var wccMutateStub = new WccMutateStub(
            genericStub,
            applicationsFacade.community().mutate(),
            applicationsFacade.community().estimate(),
            procedureReturnColumns
        );

        return new CommunityProcedureFacade(
            closeableResourceRegistry,
            procedureReturnColumns,
            applicationsFacade.community().estimate(),
            applicationsFacade.community().stats(),
            applicationsFacade.community().stream(),
            applicationsFacade.community().write(),
            approximateMaximumKCutMutateStub,
            k1ColoringMutateStub,
            kCoreMutateStub,
            kMeansMutateStub,
            labelPropagationMutateStub,
            lccMutateStub,
            leidenMutateStub,
            louvainMutateStub,
            modularityOptimizationMutateStub,
            sccMutateStub,
            triangleCountMutateStub,
            wccMutateStub,
            configurationParser
        );
    }

    public ApproximateMaximumKCutMutateStub approximateMaximumKCutMutateStub() {
        return approximateMaximumKCutMutateStub;
    }

    public Stream<ApproxMaxKCutStreamResult> approxMaxKCutStream(
        String graphName,
        Map<String, Object> configuration
    ) {

        var parsedConfig = configurationParser.parseConfiguration(configuration, ApproxMaxKCutStreamConfig::of);
        var resultBuilder = new ApproxMaxKCutResultBuilderForStreamMode(parsedConfig);

        return streamModeBusinessFacade.approximateMaximumKCut(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> approxMaxKCutStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ApproxMaxKCutStreamConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.approximateMaximumKCut(configuration, graphNameOrConfiguration));
    }

    public Stream<ConductanceStreamResult> conductanceStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ConductanceResultBuilderForStreamMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, ConductanceStreamConfig::of);
        return streamModeBusinessFacade.conductance(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public K1ColoringMutateStub k1ColoringMutateStub() {
        return k1ColoringMutateStub;
    }

    public Stream<K1ColoringStatsResult> k1ColoringStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new K1ColoringResultBuilderForStatsMode(procedureReturnColumns.contains("colorCount"));

        var parsedConfig = configurationParser.parseConfiguration(configuration, K1ColoringStatsConfig::of);
        return statsModeBusinessFacade.k1Coloring(GraphName.parse(graphName), parsedConfig, resultBuilder);

    }

    public Stream<MemoryEstimateResult> k1ColoringStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, K1ColoringStatsConfig::of);
        return Stream.of(estimationModeBusinessFacade.k1Coloring(configuration, graphNameOrConfiguration));
    }

    public Stream<K1ColoringStreamResult> k1ColoringStream(
        String graphName,
        Map<String, Object> configuration
    ) {

        var parsedConfig = configurationParser.parseConfiguration(configuration, K1ColoringStreamConfig::of);
        var resultBuilder = new K1ColoringResultBuilderForStreamMode(parsedConfig);

        return streamModeBusinessFacade.k1Coloring(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> k1ColoringStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, K1ColoringStreamConfig::of);
        return Stream.of(estimationModeBusinessFacade.k1Coloring(configuration, graphNameOrConfiguration));
    }

    public Stream<K1ColoringWriteResult> k1ColoringWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new K1ColoringResultBuilderForWriteMode(procedureReturnColumns.contains("colorCount"));

        var parsedConfig = configurationParser.parseConfiguration(configuration, K1ColoringWriteConfig::of);
        return writeModeBusinessFacade.k1Coloring(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> k1ColoringWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, K1ColoringWriteConfig::of);
        return Stream.of(estimationModeBusinessFacade.k1Coloring(configuration, graphNameOrConfiguration));
    }

    public KCoreMutateStub kCoreMutateStub() {
        return kCoreMutateStub;
    }

    public Stream<KCoreDecompositionStatsResult> kCoreStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KCoreResultBuilderForStatsMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, KCoreDecompositionStatsConfig::of);
        return statsModeBusinessFacade.kCore(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> kCoreStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            KCoreDecompositionStatsConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.kCore(configuration, graphNameOrConfiguration));
    }

    public Stream<KCoreDecompositionStreamResult> kCoreStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KCoreResultBuilderForStreamMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, KCoreDecompositionStreamConfig::of);
        return streamModeBusinessFacade.kCore(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> kCoreStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            KCoreDecompositionStreamConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.kCore(configuration, graphNameOrConfiguration));
    }

    public Stream<KCoreDecompositionWriteResult> kCoreWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KCoreResultBuilderForWriteMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, KCoreDecompositionWriteConfig::of);
        return writeModeBusinessFacade.kCore(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> kCoreWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            KCoreDecompositionWriteConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.kCore(configuration, graphNameOrConfiguration));
    }

    public KMeansMutateStub kMeansMutateStub() {
        return kMeansMutateStub;
    }

    public Stream<KmeansStatsResult> kmeansStats(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var shouldComputeListOfCentroids = procedureReturnColumns.contains("centroids");
        var resultBuilder = new KMeansResultBuilderForStatsMode(
            statisticsComputationInstructions,
            shouldComputeListOfCentroids
        );

        var parsedConfig = configurationParser.parseConfiguration(configuration, KmeansStatsConfig::of);
        return statsModeBusinessFacade.kMeans(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> kmeansStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, KmeansStatsConfig::of);
        return Stream.of(estimationModeBusinessFacade.kMeans(configuration, graphNameOrConfiguration));
    }

    public Stream<KmeansStreamResult> kmeansStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KMeansResultBuilderForStreamMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, KmeansStreamConfig::of);
        return streamModeBusinessFacade.kMeans(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> kmeansStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, KmeansStreamConfig::of);
        return Stream.of(estimationModeBusinessFacade.kMeans(configuration, graphNameOrConfiguration));
    }

    public Stream<KmeansWriteResult> kmeansWrite(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var shouldComputeListOfCentroids = procedureReturnColumns.contains("centroids");
        var resultBuilder = new KMeansResultBuilderForWriteMode(
            statisticsComputationInstructions,
            shouldComputeListOfCentroids
        );

        var parsedConfig = configurationParser.parseConfiguration(configuration, KmeansWriteConfig::of);
        return writeModeBusinessFacade.kMeans(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> kmeansWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, KmeansWriteConfig::of);
        return Stream.of(estimationModeBusinessFacade.kMeans(configuration, graphNameOrConfiguration));
    }

    public LabelPropagationMutateStub labelPropagationMutateStub() {
        return labelPropagationMutateStub;
    }

    public Stream<LabelPropagationStatsResult> labelPropagationStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LabelPropagationResultBuilderForStatsMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, LabelPropagationStatsConfig::of);
        return statsModeBusinessFacade.labelPropagation(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> labelPropagationStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            LabelPropagationStatsConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.labelPropagation(configuration, graphNameOrConfiguration));
    }

    public Stream<LabelPropagationStreamResult> labelPropagationStream(
        String graphName, Map<String, Object> configuration
    ) {

        var parsedConfig = configurationParser.parseConfiguration(configuration, LabelPropagationStreamConfig::of);
        var resultBuilder = new LabelPropagationResultBuilderForStreamMode(parsedConfig);

        return streamModeBusinessFacade.labelPropagation(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> labelPropagationStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            LabelPropagationStreamConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.labelPropagation(configuration, graphNameOrConfiguration));
    }

    public Stream<LabelPropagationWriteResult> labelPropagationWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LabelPropagationResultBuilderForWriteMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, LabelPropagationWriteConfig::of);
        return writeModeBusinessFacade.labelPropagation(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> labelPropagationWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            LabelPropagationWriteConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.labelPropagation(configuration, graphNameOrConfiguration));
    }

    public LccMutateStub lccMutateStub() {
        return lccMutateStub;
    }

    public Stream<LocalClusteringCoefficientStatsResult> localClusteringCoefficientStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new LccResultBuilderForStatsMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration,
            LocalClusteringCoefficientStatsConfig::of);
        return statsModeBusinessFacade.lcc(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            LocalClusteringCoefficientStatsConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.lcc(configuration, graphNameOrConfiguration));
    }

    public Stream<LocalClusteringCoefficientStreamResult> localClusteringCoefficientStream(
        String graphName, Map<String, Object> configuration
    ) {
        var resultBuilder = new LccResultBuilderForStreamMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration,
            LocalClusteringCoefficientStreamConfig::of);
        return streamModeBusinessFacade.lcc(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            LocalClusteringCoefficientStreamConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.lcc(configuration, graphNameOrConfiguration));
    }

    public Stream<LocalClusteringCoefficientWriteResult> localClusteringCoefficientWrite(
        String graphName, Map<String, Object> configuration
    ) {
        var resultBuilder = new LccResultBuilderForWriteMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration,
            LocalClusteringCoefficientWriteConfig::of);
        return writeModeBusinessFacade.lcc(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            LocalClusteringCoefficientWriteConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.lcc(configuration, graphNameOrConfiguration));
    }

    public LeidenMutateStub leidenMutateStub() {
        return leidenMutateStub;
    }

    public Stream<LeidenStatsResult> leidenStats(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LeidenResultBuilderForStatsMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, LeidenStatsConfig::of);
        return statsModeBusinessFacade.leiden(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> leidenStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, LeidenStatsConfig::of);
        return Stream.of(estimationModeBusinessFacade.leiden(configuration, graphNameOrConfiguration));
    }

    public Stream<LeidenStreamResult> leidenStream(
        String graphName,
        Map<String, Object> configuration
    ) {

        var parsedConfig = configurationParser.parseConfiguration(configuration, LeidenStreamConfig::of);
        var resultBuilder = new LeidenResultBuilderForStreamMode(parsedConfig);

        return streamModeBusinessFacade.leiden(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> leidenStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, LeidenStreamConfig::of);
        return Stream.of(estimationModeBusinessFacade.leiden(configuration, graphNameOrConfiguration));
    }

    public Stream<LeidenWriteResult> leidenWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LeidenResultBuilderForWriteMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, LeidenWriteConfig::of);
        return writeModeBusinessFacade.leiden(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> leidenWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, LeidenWriteConfig::of);
        return Stream.of(estimationModeBusinessFacade.leiden(configuration, graphNameOrConfiguration));
    }

    public LouvainMutateStub louvainMutateStub() {
        return louvainMutateStub;
    }

    public Stream<LouvainStatsResult> louvainStats(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LouvainResultBuilderForStatsMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, LouvainStatsConfig::of);
        return statsModeBusinessFacade.louvain(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> louvainStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, LouvainStatsConfig::of);
        return Stream.of(estimationModeBusinessFacade.louvain(configuration, graphNameOrConfiguration));
    }

    public Stream<LouvainStreamResult> louvainStream(
        String graphName,
        Map<String, Object> configuration
    ) {

        var parsedConfig = configurationParser.parseConfiguration(configuration, LouvainStreamConfig::of);
        var resultBuilder = new LouvainResultBuilderForStreamMode(parsedConfig);

        return streamModeBusinessFacade.louvain(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> louvainStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, LouvainStreamConfig::of);
        return Stream.of(estimationModeBusinessFacade.louvain(configuration, graphNameOrConfiguration));
    }

    public Stream<LouvainWriteResult> louvainWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LouvainResultBuilderForWriteMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, LouvainWriteConfig::of);
        return writeModeBusinessFacade.louvain(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> louvainWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, LouvainWriteConfig::of);
        return Stream.of(estimationModeBusinessFacade.louvain(configuration, graphNameOrConfiguration));
    }

    public Stream<ModularityStatsResult> modularityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ModularityResultBuilderForStatsMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, ModularityStatsConfig::of);
        return statsModeBusinessFacade.modularity(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> modularityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, ModularityStatsConfig::of);
        return Stream.of(estimationModeBusinessFacade.modularity(configuration, graphNameOrConfiguration));
    }

    public Stream<ModularityStreamResult> modularityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ModularityResultBuilderForStreamMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, ModularityStreamConfig::of);
        return streamModeBusinessFacade.modularity(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> modularityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, ModularityStreamConfig::of);
        return Stream.of(estimationModeBusinessFacade.modularity(configuration, graphNameOrConfiguration));
    }

    public ModularityOptimizationMutateStub modularityOptimizationMutateStub() {
        return modularityOptimizationMutateStub;
    }

    public Stream<ModularityOptimizationStatsResult> modularityOptimizationStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        StatisticsComputationInstructions statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new ModularityOptimizationResultBuilderForStatsMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, ModularityOptimizationStatsConfig::of);
        return statsModeBusinessFacade.modularityOptimization(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> modularityOptimizationStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ModularityOptimizationStatsConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.modularityOptimization(configuration, graphNameOrConfiguration));
    }

    public Stream<ModularityOptimizationStreamResult> modularityOptimizationStream(
        String graphName,
        Map<String, Object> configuration
    ) {

        var parsedConfig = configurationParser.parseConfiguration(configuration,
            ModularityOptimizationStreamConfig::of);

        var resultBuilder = new ModularityOptimizationResultBuilderForStreamMode(parsedConfig);

        return streamModeBusinessFacade.modularityOptimization(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> modularityOptimizationStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ModularityOptimizationStreamConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.modularityOptimization(configuration, graphNameOrConfiguration));
    }

    public Stream<ModularityOptimizationWriteResult> modularityOptimizationWrite(
        String graphName, Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new ModularityOptimizationResultBuilderForWriteMode(statisticsComputationInstructions);


        var parsedConfig = configurationParser.parseConfiguration(configuration, ModularityOptimizationWriteConfig::of);
        return writeModeBusinessFacade.modularityOptimization(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> modularityOptimizationWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ModularityOptimizationWriteConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.modularityOptimization(configuration, graphNameOrConfiguration));
    }

    public SccMutateStub sccMutateStub() {
        return sccMutateStub;
    }

    public Stream<SccStatsResult> sccStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forComponents(
            procedureReturnColumns);
        var resultBuilder = new SccResultBuilderForStatsMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, SccStatsConfig::of);
        return statsModeBusinessFacade.scc(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> sccStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, SccStatsConfig::of);
        return Stream.of(estimationModeBusinessFacade.scc(configuration, graphNameOrConfiguration));
    }

    public Stream<SccStreamResult> sccStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var parsedConfig = configurationParser.parseConfiguration(configuration, SccStreamConfig::of);
        var resultBuilder = new SccResultBuilderForStreamMode(parsedConfig);

        return streamModeBusinessFacade.scc(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> sccStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, SccStreamConfig::of);
        return Stream.of(estimationModeBusinessFacade.scc(configuration, graphNameOrConfiguration));
    }

    public Stream<SccWriteResult> sccWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forComponents(
            procedureReturnColumns);
        var resultBuilder = new SccResultBuilderForWriteMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, SccWriteConfig::of);
        return writeModeBusinessFacade.scc(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<AlphaSccWriteResult> sccWriteAlpha(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = new ProcedureStatisticsComputationInstructions(true, true);
        var resultBuilder = new SccAlphaResultBuilderForWriteMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, SccAlphaWriteConfig::of);
        return writeModeBusinessFacade.sccAlpha(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> sccWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, SccWriteConfig::of);
        return Stream.of(estimationModeBusinessFacade.scc(configuration, graphNameOrConfiguration));
    }

    public TriangleCountMutateStub triangleCountMutateStub() {
        return triangleCountMutateStub;
    }

    public Stream<TriangleCountStatsResult> triangleCountStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new TriangleCountResultBuilderForStatsMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, TriangleCountStatsConfig::of);
        return statsModeBusinessFacade.triangleCount(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> triangleCountStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            TriangleCountStatsConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.triangleCount(configuration, graphNameOrConfiguration));
    }

    public Stream<TriangleCountStreamResult> triangleCountStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new TriangleCountResultBuilderForStreamMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, TriangleCountStreamConfig::of);
        return streamModeBusinessFacade.triangleCount(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> triangleCountStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            TriangleCountStreamConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.triangleCount(configuration, graphNameOrConfiguration));
    }

    public Stream<TriangleCountWriteResult> triangleCountWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new TriangleCountResultBuilderForWriteMode();

        var parsedConfig = configurationParser.parseConfiguration(configuration, TriangleCountWriteConfig::of);
        return writeModeBusinessFacade.triangleCount(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> triangleCountWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            TriangleCountWriteConfig::of
        );
        return Stream.of(estimationModeBusinessFacade.triangleCount(configuration, graphNameOrConfiguration));
    }

    public Stream<TriangleStreamResult> trianglesStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new TrianglesResultBuilderForStreamMode(closeableResourceRegistry);

        var parsedConfig = configurationParser.parseConfiguration(configuration, TriangleCountBaseConfig::of);
        return streamModeBusinessFacade.triangles(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public WccMutateStub wccMutateStub() {
        return wccMutateStub;
    }

    public Stream<WccStatsResult> wccStats(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forComponents(
            procedureReturnColumns);
        var resultBuilder = new WccResultBuilderForStatsMode(statisticsComputationInstructions);

        var parsedConfig = configurationParser.parseConfiguration(configuration, WccStatsConfig::of);
        return statsModeBusinessFacade.wcc(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> wccStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var config = configurationParser.parseConfiguration(algorithmConfiguration, WccStatsConfig::of);
        return Stream.of(estimationModeBusinessFacade.wcc(config, graphNameOrConfiguration));
    }

    public Stream<WccStreamResult> wccStream(String graphName, Map<String, Object> configuration) {
        var parsedConfig = configurationParser.parseConfiguration(configuration, WccStreamConfig::of);
        var resultBuilder = new WccResultBuilderForStreamMode(parsedConfig);

        return streamModeBusinessFacade.wcc(GraphName.parse(graphName), parsedConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> wccStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        var config = configurationParser.parseConfiguration(algorithmConfiguration, WccStreamConfig::of);
        return Stream.of(estimationModeBusinessFacade.wcc(config, graphNameOrConfiguration));

    }

    public Stream<WccWriteResult> wccWrite(String graphName, Map<String, Object> configuration) {

        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions
            .forComponents(procedureReturnColumns);

        var resultBuilder = new WccResultBuilderForWriteMode(statisticsComputationInstructions);
        var writeConfig = configurationParser.parseConfiguration(configuration, WccWriteConfig::of);

        return writeModeBusinessFacade.wcc(GraphName.parse(graphName), writeConfig, resultBuilder);
    }

    public Stream<MemoryEstimateResult> wccWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        var configuration = configurationParser.parseConfiguration(algorithmConfiguration, WccWriteConfig::of);
        return Stream.of(estimationModeBusinessFacade.wcc(configuration, graphNameOrConfiguration));
    }
}
