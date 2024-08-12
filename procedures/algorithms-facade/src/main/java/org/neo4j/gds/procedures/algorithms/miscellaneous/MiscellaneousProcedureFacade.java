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
package org.neo4j.gds.procedures.algorithms.miscellaneous;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsWriteModeBusinessFacade;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.CollapsePathMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.ScalePropertiesMutateStub;
import org.neo4j.gds.procedures.algorithms.runners.AlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.scaleproperties.ScalePropertiesStatsConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesStreamConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesWriteConfig;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public final class MiscellaneousProcedureFacade {
    private final ProcedureReturnColumns procedureReturnColumns;

    private final ScalePropertiesMutateStub alphaScalePropertiesMutateStub;
    private final CollapsePathMutateStub collapsePathMutateStub;
    private final ScalePropertiesMutateStub scalePropertiesMutateStub;

    private final ApplicationsFacade applicationsFacade;

    private final EstimationModeRunner estimationMode;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;

    private MiscellaneousProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        ScalePropertiesMutateStub alphaScalePropertiesMutateStub,
        CollapsePathMutateStub collapsePathMutateStub,
        ScalePropertiesMutateStub scalePropertiesMutateStub,
        ApplicationsFacade applicationsFacade,
        EstimationModeRunner estimationMode,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.alphaScalePropertiesMutateStub = alphaScalePropertiesMutateStub;
        this.collapsePathMutateStub = collapsePathMutateStub;
        this.scalePropertiesMutateStub = scalePropertiesMutateStub;
        this.applicationsFacade = applicationsFacade;
        this.estimationMode = estimationMode;
        this.algorithmExecutionScaffolding = algorithmExecutionScaffolding;
    }

    public static MiscellaneousProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns,
        EstimationModeRunner estimationModeRunner,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding) {
        var alphaScalePropertiesMutateStub = new ScalePropertiesMutateStub(
            genericStub,
            applicationsFacade,
            procedureReturnColumns,
            true
        );
        var collapsePathMutateStub = new CollapsePathMutateStub(genericStub, applicationsFacade);
        var scalePropertiesMutateStub = new ScalePropertiesMutateStub(
            genericStub,
            applicationsFacade,
            procedureReturnColumns,
            false
        );

        return new MiscellaneousProcedureFacade(
            procedureReturnColumns,
            alphaScalePropertiesMutateStub,
            collapsePathMutateStub,
            scalePropertiesMutateStub,
            applicationsFacade,
            estimationModeRunner,
            algorithmExecutionScaffolding);
    }

    public ScalePropertiesMutateStub alphaScalePropertiesMutateStub() {
        return alphaScalePropertiesMutateStub;
    }

    public Stream<ScalePropertiesStreamResult> alphaScalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ScalePropertiesResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithmWithValidation(
            graphName,
            configuration,
            ScalePropertiesStreamConfig::of,
            Optional.of(new ScalePropertiesConfigurationValidationHook<>(true)),
            streamMode()::scaleProperties,
            resultBuilder
        );
    }

    public CollapsePathMutateStub collapsePathMutateStub() {
        return collapsePathMutateStub;
    }

    public ScalePropertiesMutateStub scalePropertiesMutateStub() {
        return scalePropertiesMutateStub;
    }

    public Stream<ScalePropertiesStatsResult> scalePropertiesStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var validationHook = new ScalePropertiesConfigurationValidationHook<ScalePropertiesStatsConfig>(false);

        var shouldDisplayScalerStatistics = procedureReturnColumns.contains("scalerStatistics");
        var resultBuilder = new ScalePropertiesResultBuilderForStatsMode(shouldDisplayScalerStatistics);

        return algorithmExecutionScaffolding.runAlgorithmWithValidation(
            graphName,
            configuration,
            ScalePropertiesStatsConfig::of,
            Optional.of(validationHook),
            statsMode()::scaleProperties,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> scalePropertiesStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ScalePropertiesStatsConfig::of,
            configuration -> estimationMode().scaleProperties(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<ScalePropertiesStreamResult> scalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ScalePropertiesResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithmWithValidation(
            graphName,
            configuration,
            ScalePropertiesStreamConfig::of,
            Optional.of(new ScalePropertiesConfigurationValidationHook<>(false)),
            streamMode()::scaleProperties,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> scalePropertiesStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ScalePropertiesStreamConfig::of,
            configuration -> estimationMode().scaleProperties(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<ScalePropertiesWriteResult> scalePropertiesWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var validationHook = new ScalePropertiesConfigurationValidationHook<ScalePropertiesWriteConfig>(false);

        var shouldDisplayScalerStatistics = procedureReturnColumns.contains("scalerStatistics");
        var resultBuilder = new ScalePropertiesResultBuilderForWriteMode(shouldDisplayScalerStatistics);

        return algorithmExecutionScaffolding.runAlgorithmWithValidation(
            graphName,
            configuration,
            ScalePropertiesWriteConfig::of,
            Optional.of(validationHook),
            writeMode()::scaleProperties,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> scalePropertiesWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ScalePropertiesWriteConfig::of,
            configuration -> estimationMode().scaleProperties(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    private MiscellaneousApplicationsEstimationModeBusinessFacade estimationMode() {
        return applicationsFacade.miscellaneous().estimate();
    }

    private MiscellaneousApplicationsStatsModeBusinessFacade statsMode() {
        return applicationsFacade.miscellaneous().stats();
    }

    private MiscellaneousApplicationsStreamModeBusinessFacade streamMode() {
        return applicationsFacade.miscellaneous().stream();
    }

    private MiscellaneousApplicationsWriteModeBusinessFacade writeMode() {
        return applicationsFacade.miscellaneous().write();
    }
}
