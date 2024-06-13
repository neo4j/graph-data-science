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
import org.neo4j.gds.procedures.algorithms.community.stubs.ApproximateMaximumKCutMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.K1ColoringMutateStub;
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
    private final WccMutateStub wccMutateStub;

    private final ApplicationsFacade applicationsFacade;

    private final EstimationModeRunner estimationMode;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffoldingForStreamMode;

    private CommunityProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        ApproximateMaximumKCutMutateStub approximateMaximumKCutMutateStub,
        K1ColoringMutateStub k1ColoringMutateStub,
        WccMutateStub wccMutateStub,
        ApplicationsFacade applicationsFacade,
        EstimationModeRunner estimationMode,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding,
        AlgorithmExecutionScaffolding algorithmExecutionScaffoldingForStreamMode
    ) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.approximateMaximumKCutMutateStub = approximateMaximumKCutMutateStub;
        this.k1ColoringMutateStub = k1ColoringMutateStub;
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
        var wccMutateStub = new WccMutateStub(genericStub, applicationsFacade, procedureReturnColumns);

        return new CommunityProcedureFacade(
            procedureReturnColumns,
            approximateMaximumKCutMutateStub,
            k1ColoringMutateStub,
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
