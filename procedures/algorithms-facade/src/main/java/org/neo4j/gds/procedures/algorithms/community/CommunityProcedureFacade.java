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
import org.neo4j.gds.procedures.algorithms.community.stubs.ApproximateMaximumKCutMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.K1ColoringMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.KCoreMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.KMeansMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LabelPropagationMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.WccMutateStub;
import org.neo4j.gds.procedures.algorithms.runners.AlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.wcc.WccStatsConfig;
import org.neo4j.gds.wcc.WccStreamConfig;
import org.neo4j.gds.wcc.WccWriteConfig;

import java.util.Map;
import java.util.stream.Stream;

public final class CommunityProcedureFacade {
    private final ProcedureReturnColumns procedureReturnColumns;
    private final ApproximateMaximumKCutMutateStub approximateMaximumKCutMutateStub;
    private final K1ColoringMutateStub k1ColoringMutateStub;
    private final KCoreMutateStub kCoreMutateStub;
    private final KMeansMutateStub kMeansMutateStub;
    private final LabelPropagationMutateStub labelPropagationMutateStub;
    private final WccMutateStub wccMutateStub;

    private final ApplicationsFacade applicationsFacade;

    private final EstimationModeRunner estimationMode;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffoldingForStreamMode;

    private CommunityProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        ApproximateMaximumKCutMutateStub approximateMaximumKCutMutateStub,
        K1ColoringMutateStub k1ColoringMutateStub,
        KCoreMutateStub kCoreMutateStub,
        KMeansMutateStub kMeansMutateStub,
        LabelPropagationMutateStub labelPropagationMutateStub,
        WccMutateStub wccMutateStub,
        ApplicationsFacade applicationsFacade,
        EstimationModeRunner estimationMode,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding,
        AlgorithmExecutionScaffolding algorithmExecutionScaffoldingForStreamMode
    ) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.approximateMaximumKCutMutateStub = approximateMaximumKCutMutateStub;
        this.k1ColoringMutateStub = k1ColoringMutateStub;
        this.kCoreMutateStub = kCoreMutateStub;
        this.kMeansMutateStub = kMeansMutateStub;
        this.labelPropagationMutateStub = labelPropagationMutateStub;
        this.wccMutateStub = wccMutateStub;
        this.applicationsFacade = applicationsFacade;
        this.estimationMode = estimationMode;
        this.algorithmExecutionScaffolding = algorithmExecutionScaffolding;
        this.algorithmExecutionScaffoldingForStreamMode = algorithmExecutionScaffoldingForStreamMode;
    }

    public static CommunityProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns,
        EstimationModeRunner estimationModeRunner,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding,
        AlgorithmExecutionScaffolding algorithmExecutionScaffoldingForStreamMode
    ) {
        var approximateMaximumKCutMutateStub = new ApproximateMaximumKCutMutateStub(genericStub, applicationsFacade);
        var k1ColoringMutateStub = new K1ColoringMutateStub(genericStub, applicationsFacade, procedureReturnColumns);
        var kCoreMutateStub = new KCoreMutateStub(genericStub, applicationsFacade);
        var kMeansMutateStub = new KMeansMutateStub(genericStub, applicationsFacade, procedureReturnColumns);
        var labelPropagationMutateStub = new LabelPropagationMutateStub(
            genericStub,
            applicationsFacade,
            procedureReturnColumns
        );
        var wccMutateStub = new WccMutateStub(genericStub, applicationsFacade, procedureReturnColumns);

        return new CommunityProcedureFacade(
            procedureReturnColumns,
            approximateMaximumKCutMutateStub,
            k1ColoringMutateStub,
            kCoreMutateStub,
            kMeansMutateStub,
            labelPropagationMutateStub,
            wccMutateStub,
            applicationsFacade,
            estimationModeRunner,
            algorithmExecutionScaffolding,
            algorithmExecutionScaffoldingForStreamMode
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

        return algorithmExecutionScaffoldingForStreamMode.runAlgorithm(
            graphName,
            configuration,
            ApproxMaxKCutStreamConfig::of,
            streamMode()::approximateMaximumKCut,
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
            configuration -> estimationMode().approximateMaximumKCut(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<ConductanceStreamResult> conductanceStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ConductanceResultBuilderForStreamMode();

        return algorithmExecutionScaffoldingForStreamMode.runAlgorithm(
            graphName,
            configuration,
            ConductanceStreamConfig::of,
            streamMode()::conductance,
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

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            K1ColoringStatsConfig::of,
            statsMode()::k1Coloring,
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
            configuration -> estimationMode().k1Coloring(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<K1ColoringStreamResult> k1ColoringStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new K1ColoringResultBuilderForStreamMode();

        return algorithmExecutionScaffoldingForStreamMode.runAlgorithm(
            graphName,
            configuration,
            K1ColoringStreamConfig::of,
            streamMode()::k1Coloring,
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
            configuration -> estimationMode().k1Coloring(configuration, graphNameOrConfiguration)
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
            writeMode()::k1Coloring,
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
            configuration -> estimationMode().k1Coloring(configuration, graphNameOrConfiguration)
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

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            KCoreDecompositionStatsConfig::of,
            statsMode()::kCore,
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
            configuration -> estimationMode().kCore(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<KCoreDecompositionStreamResult> kCoreStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KCoreResultBuilderForStreamMode();

        return algorithmExecutionScaffoldingForStreamMode.runAlgorithm(
            graphName,
            configuration,
            KCoreDecompositionStreamConfig::of,
            streamMode()::kCore,
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
            configuration -> estimationMode().kCore(configuration, graphNameOrConfiguration)
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
            writeMode()::kCore,
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
            configuration -> estimationMode().kCore(configuration, graphNameOrConfiguration)
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

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            KmeansStatsConfig::of,
            statsMode()::kMeans,
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
            configuration -> estimationMode().kMeans(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<KmeansStreamResult> kmeansStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KMeansResultBuilderForStreamMode();

        return algorithmExecutionScaffoldingForStreamMode.runAlgorithm(
            graphName,
            configuration,
            KmeansStreamConfig::of,
            streamMode()::kMeans,
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
            configuration -> estimationMode().kMeans(configuration, graphNameOrConfiguration)
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
            writeMode()::kMeans,
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
            configuration -> estimationMode().kMeans(configuration, graphNameOrConfiguration)
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

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            LabelPropagationStatsConfig::of,
            statsMode()::labelPropagation,
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
            configuration -> estimationMode().labelPropagation(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<LabelPropagationStreamResult> labelPropagationStream(
        String graphName, Map<String, Object> configuration
    ) {
        var resultBuilder = new LabelPropagationResultBuilderForStreamMode();

        return algorithmExecutionScaffoldingForStreamMode.runAlgorithm(
            graphName,
            configuration,
            LabelPropagationStreamConfig::of,
            streamMode()::labelPropagation,
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
            configuration -> estimationMode().labelPropagation(configuration, graphNameOrConfiguration)
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
            writeMode()::labelPropagation,
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
            configuration -> estimationMode().labelPropagation(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public WccMutateStub wccMutateStub() {
        return wccMutateStub;
    }

    public Stream<WccStatsResult> wccStats(String graphName, Map<String, Object> configuration) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forComponents(
            procedureReturnColumns);
        var resultBuilder = new WccResultBuilderForStatsMode(statisticsComputationInstructions);

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            WccStatsConfig::of,
            statsMode()::wcc,
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
            configuration -> estimationMode().wcc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<WccStreamResult> wccStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new WccResultBuilderForStreamMode();

        return algorithmExecutionScaffoldingForStreamMode.runAlgorithm(
            graphName,
            configuration,
            WccStreamConfig::of,
            streamMode()::wcc,
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
            configuration -> estimationMode().wcc(configuration, graphNameOrConfiguration)
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
            writeMode()::wcc,
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
            configuration -> estimationMode().wcc(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    private CommunityAlgorithmsEstimationModeBusinessFacade estimationMode() {
        return applicationsFacade.community().estimate();
    }

    private CommunityAlgorithmsStatsModeBusinessFacade statsMode() {
        return applicationsFacade.community().stats();
    }

    private CommunityAlgorithmsStreamModeBusinessFacade streamMode() {
        return applicationsFacade.community().stream();
    }

    private CommunityAlgorithmsWriteModeBusinessFacade writeMode() {
        return applicationsFacade.community().write();
    }
}
