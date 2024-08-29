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
import org.neo4j.gds.procedures.algorithms.runners.AlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
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

    private final EstimationModeRunner estimationMode;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;

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
        EstimationModeRunner estimationMode,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding
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
        this.estimationMode = estimationMode;
        this.algorithmExecutionScaffolding = algorithmExecutionScaffolding;
    }

    public static CommunityProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        CloseableResourceRegistry closeableResourceRegistry,
        ProcedureReturnColumns procedureReturnColumns,
        EstimationModeRunner estimationModeRunner,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding
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
            wccMutateStub, estimationModeRunner, algorithmExecutionScaffolding
        );
    }

    public ApproximateMaximumKCutMutateStub approximateMaximumKCutMutateStub() {
        return approximateMaximumKCutMutateStub;
    }

    public Stream<ApproxMaxKCutStreamResult> approxMaxKCutStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ApproxMaxKCutResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            ApproxMaxKCutStreamConfig::of,
            streamModeBusinessFacade::approximateMaximumKCut,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> approxMaxKCutStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ApproxMaxKCutStreamConfig::of,
            configuration -> estimationModeBusinessFacade.approximateMaximumKCut(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<ConductanceStreamResult> conductanceStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ConductanceResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            ConductanceStreamConfig::of,
            streamModeBusinessFacade::conductance,
            resultBuilder
        );
    }

    public K1ColoringMutateStub k1ColoringMutateStub() {
        return k1ColoringMutateStub;
    }

    public Stream<K1ColoringStatsResult> k1ColoringStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new K1ColoringResultBuilderForStatsMode(procedureReturnColumns.contains("colorCount"));

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            K1ColoringStatsConfig::of,
            statsModeBusinessFacade::k1Coloring,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> k1ColoringStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            K1ColoringStatsConfig::of,
            configuration -> estimationModeBusinessFacade.k1Coloring(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<K1ColoringStreamResult> k1ColoringStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new K1ColoringResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            K1ColoringStreamConfig::of,
            streamModeBusinessFacade::k1Coloring,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> k1ColoringStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            K1ColoringStreamConfig::of,
            configuration -> estimationModeBusinessFacade.k1Coloring(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<K1ColoringWriteResult> k1ColoringWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new K1ColoringResultBuilderForWriteMode(procedureReturnColumns.contains("colorCount"));

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            K1ColoringWriteConfig::of,
            writeModeBusinessFacade::k1Coloring,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> k1ColoringWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            K1ColoringWriteConfig::of,
            configuration -> estimationModeBusinessFacade.k1Coloring(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public KCoreMutateStub kCoreMutateStub() {
        return kCoreMutateStub;
    }

    public Stream<KCoreDecompositionStatsResult> kCoreStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KCoreResultBuilderForStatsMode();

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            KCoreDecompositionStatsConfig::of,
            statsModeBusinessFacade::kCore,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> kCoreStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            KCoreDecompositionStatsConfig::of,
            configuration -> estimationModeBusinessFacade.kCore(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<KCoreDecompositionStreamResult> kCoreStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KCoreResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            KCoreDecompositionStreamConfig::of,
            streamModeBusinessFacade::kCore,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> kCoreStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            KCoreDecompositionStreamConfig::of,
            configuration -> estimationModeBusinessFacade.kCore(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<KCoreDecompositionWriteResult> kCoreWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KCoreResultBuilderForWriteMode();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            KCoreDecompositionWriteConfig::of,
            writeModeBusinessFacade::kCore,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> kCoreWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            KCoreDecompositionWriteConfig::of,
            configuration -> estimationModeBusinessFacade.kCore(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            KmeansStatsConfig::of,
            statsModeBusinessFacade::kMeans,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> kmeansStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            KmeansStatsConfig::of,
            configuration -> estimationModeBusinessFacade.kMeans(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<KmeansStreamResult> kmeansStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KMeansResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            KmeansStreamConfig::of,
            streamModeBusinessFacade::kMeans,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> kmeansStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            KmeansStreamConfig::of,
            configuration -> estimationModeBusinessFacade.kMeans(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<KmeansWriteResult> kmeansWrite(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var shouldComputeListOfCentroids = procedureReturnColumns.contains("centroids");
        var resultBuilder = new KMeansResultBuilderForWriteMode(
            statisticsComputationInstructions,
            shouldComputeListOfCentroids
        );

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            KmeansWriteConfig::of,
            writeModeBusinessFacade::kMeans,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> kmeansWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            KmeansWriteConfig::of,
            configuration -> estimationModeBusinessFacade.kMeans(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            LabelPropagationStatsConfig::of,
            statsModeBusinessFacade::labelPropagation,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> labelPropagationStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LabelPropagationStatsConfig::of,
            configuration -> estimationModeBusinessFacade.labelPropagation(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<LabelPropagationStreamResult> labelPropagationStream(
        String graphName, Map<String, Object> configuration
    ) {
        var resultBuilder = new LabelPropagationResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            LabelPropagationStreamConfig::of,
            streamModeBusinessFacade::labelPropagation,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> labelPropagationStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LabelPropagationStreamConfig::of,
            configuration -> estimationModeBusinessFacade.labelPropagation(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<LabelPropagationWriteResult> labelPropagationWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LabelPropagationResultBuilderForWriteMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            LabelPropagationWriteConfig::of,
            writeModeBusinessFacade::labelPropagation,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> labelPropagationWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LabelPropagationWriteConfig::of,
            configuration -> estimationModeBusinessFacade.labelPropagation(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public LccMutateStub lccMutateStub() {
        return lccMutateStub;
    }

    public Stream<LocalClusteringCoefficientStatsResult> localClusteringCoefficientStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new LccResultBuilderForStatsMode();

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            LocalClusteringCoefficientStatsConfig::of,
            statsModeBusinessFacade::lcc,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LocalClusteringCoefficientStatsConfig::of,
            configuration -> estimationModeBusinessFacade.lcc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<LocalClusteringCoefficientStreamResult> localClusteringCoefficientStream(
        String graphName, Map<String, Object> configuration
    ) {
        var resultBuilder = new LccResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            LocalClusteringCoefficientStreamConfig::of,
            streamModeBusinessFacade::lcc,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LocalClusteringCoefficientStreamConfig::of,
            configuration -> estimationModeBusinessFacade.lcc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<LocalClusteringCoefficientWriteResult> localClusteringCoefficientWrite(
        String graphName, Map<String, Object> configuration
    ) {
        var resultBuilder = new LccResultBuilderForWriteMode();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            LocalClusteringCoefficientWriteConfig::of,
            writeModeBusinessFacade::lcc,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LocalClusteringCoefficientWriteConfig::of,
            configuration -> estimationModeBusinessFacade.lcc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public LeidenMutateStub leidenMutateStub() {
        return leidenMutateStub;
    }

    public Stream<LeidenStatsResult> leidenStats(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LeidenResultBuilderForStatsMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            LeidenStatsConfig::of,
            statsModeBusinessFacade::leiden,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> leidenStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LeidenStatsConfig::of,
            configuration -> estimationModeBusinessFacade.leiden(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<LeidenStreamResult> leidenStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new LeidenResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            LeidenStreamConfig::of,
            streamModeBusinessFacade::leiden,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> leidenStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LeidenStreamConfig::of,
            configuration -> estimationModeBusinessFacade.leiden(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<LeidenWriteResult> leidenWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LeidenResultBuilderForWriteMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            LeidenWriteConfig::of,
            writeModeBusinessFacade::leiden,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> leidenWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LeidenWriteConfig::of,
            configuration -> estimationModeBusinessFacade.leiden(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public LouvainMutateStub louvainMutateStub() {
        return louvainMutateStub;
    }

    public Stream<LouvainStatsResult> louvainStats(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LouvainResultBuilderForStatsMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            LouvainStatsConfig::of,
            statsModeBusinessFacade::louvain,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> louvainStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LouvainStatsConfig::of,
            configuration -> estimationModeBusinessFacade.louvain(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<LouvainStreamResult> louvainStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new LouvainResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            LouvainStreamConfig::of,
            streamModeBusinessFacade::louvain,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> louvainStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LouvainStreamConfig::of,
            configuration -> estimationModeBusinessFacade.louvain(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<LouvainWriteResult> louvainWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new LouvainResultBuilderForWriteMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            LouvainWriteConfig::of,
            writeModeBusinessFacade::louvain,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> louvainWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            LouvainWriteConfig::of,
            configuration -> estimationModeBusinessFacade.louvain(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<ModularityStatsResult> modularityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ModularityResultBuilderForStatsMode();

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            ModularityStatsConfig::of,
            statsModeBusinessFacade::modularity,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> modularityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ModularityStatsConfig::of,
            configuration -> estimationModeBusinessFacade.modularity(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<ModularityStreamResult> modularityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ModularityResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            ModularityStreamConfig::of,
            streamModeBusinessFacade::modularity,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> modularityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ModularityStreamConfig::of,
            configuration -> estimationModeBusinessFacade.modularity(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            ModularityOptimizationStatsConfig::of,
            statsModeBusinessFacade::modularityOptimization,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> modularityOptimizationStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ModularityOptimizationStatsConfig::of,
            configuration -> estimationModeBusinessFacade.modularityOptimization(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<ModularityOptimizationStreamResult> modularityOptimizationStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ModularityOptimizationResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            ModularityOptimizationStreamConfig::of,
            streamModeBusinessFacade::modularityOptimization,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> modularityOptimizationStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ModularityOptimizationStreamConfig::of,
            configuration -> estimationModeBusinessFacade.modularityOptimization(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<ModularityOptimizationWriteResult> modularityOptimizationWrite(
        String graphName, Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var resultBuilder = new ModularityOptimizationResultBuilderForWriteMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            ModularityOptimizationWriteConfig::of,
            writeModeBusinessFacade::modularityOptimization,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> modularityOptimizationWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ModularityOptimizationWriteConfig::of,
            configuration -> estimationModeBusinessFacade.modularityOptimization(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            SccStatsConfig::of,
            statsModeBusinessFacade::scc,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> sccStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            SccStatsConfig::of,
            configuration -> estimationModeBusinessFacade.scc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<SccStreamResult> sccStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new SccResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            SccStreamConfig::of,
            streamModeBusinessFacade::scc,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> sccStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            SccStreamConfig::of,
            configuration -> estimationModeBusinessFacade.scc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<SccWriteResult> sccWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forComponents(
            procedureReturnColumns);
        var resultBuilder = new SccResultBuilderForWriteMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            SccWriteConfig::of,
            writeModeBusinessFacade::scc,
            resultBuilder
        );
    }

    public Stream<AlphaSccWriteResult> sccWriteAlpha(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statisticsComputationInstructions = new ProcedureStatisticsComputationInstructions(true, true);
        var resultBuilder = new SccAlphaResultBuilderForWriteMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            SccAlphaWriteConfig::of,
            writeModeBusinessFacade::sccAlpha,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> sccWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            SccWriteConfig::of,
            configuration -> estimationModeBusinessFacade.scc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public TriangleCountMutateStub triangleCountMutateStub() {
        return triangleCountMutateStub;
    }

    public Stream<TriangleCountStatsResult> triangleCountStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new TriangleCountResultBuilderForStatsMode();

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            TriangleCountStatsConfig::of,
            statsModeBusinessFacade::triangleCount,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> triangleCountStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            TriangleCountStatsConfig::of,
            configuration -> estimationModeBusinessFacade.triangleCount(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<TriangleCountStreamResult> triangleCountStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new TriangleCountResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            TriangleCountStreamConfig::of,
            streamModeBusinessFacade::triangleCount,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> triangleCountStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            TriangleCountStreamConfig::of,
            configuration -> estimationModeBusinessFacade.triangleCount(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<TriangleCountWriteResult> triangleCountWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new TriangleCountResultBuilderForWriteMode();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            TriangleCountWriteConfig::of,
            writeModeBusinessFacade::triangleCount,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> triangleCountWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            TriangleCountWriteConfig::of,
            configuration -> estimationModeBusinessFacade.triangleCount(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<TriangleStreamResult> trianglesStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new TrianglesResultBuilderForStreamMode(closeableResourceRegistry);

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            TriangleCountBaseConfig::of,
            streamModeBusinessFacade::triangles,
            resultBuilder
        );
    }

    public WccMutateStub wccMutateStub() {
        return wccMutateStub;
    }

    public Stream<WccStatsResult> wccStats(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forComponents(
            procedureReturnColumns);
        var resultBuilder = new WccResultBuilderForStatsMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            WccStatsConfig::of,
            statsModeBusinessFacade::wcc,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> wccStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            WccStatsConfig::of,
            configuration -> estimationModeBusinessFacade.wcc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<WccStreamResult> wccStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new WccResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            WccStreamConfig::of,
            streamModeBusinessFacade::wcc,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> wccStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            WccStreamConfig::of,
            configuration -> estimationModeBusinessFacade.wcc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<WccWriteResult> wccWrite(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forComponents(
            procedureReturnColumns);
        var resultBuilder = new WccResultBuilderForWriteMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            WccWriteConfig::of,
            writeModeBusinessFacade::wcc,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> wccWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            WccWriteConfig::of,
            configuration -> estimationModeBusinessFacade.wcc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

}
