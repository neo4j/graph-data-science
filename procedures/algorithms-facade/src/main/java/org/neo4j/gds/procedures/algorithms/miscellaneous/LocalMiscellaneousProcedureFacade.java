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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsWriteModeBusinessFacade;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.LocalCollapsePathMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.LocalIndexInverseMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.LocalScalePropertiesMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.LocalToUndirectedMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.MiscellaneousStubs;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.scaleproperties.AlphaScalePropertiesMutateConfig;
import org.neo4j.gds.scaleproperties.AlphaScalePropertiesStreamConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesMutateConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesStatsConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesStreamConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesWriteConfig;

import java.util.Map;
import java.util.stream.Stream;

public final class LocalMiscellaneousProcedureFacade implements MiscellaneousProcedureFacade {
    private final ProcedureReturnColumns procedureReturnColumns;

    //stubs
    private final MiscellaneousStubs stubs;

    //
    private final MiscellaneousApplicationsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final MiscellaneousApplicationsStatsModeBusinessFacade statsModeBusinessFacade;
    private final MiscellaneousApplicationsStreamModeBusinessFacade streamModeBusinessFacade;
    private final MiscellaneousApplicationsWriteModeBusinessFacade writeModeBusinessFacade;

    private final UserSpecificConfigurationParser configurationParser;


    private LocalMiscellaneousProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        MiscellaneousStubs stubs,
        MiscellaneousApplicationsEstimationModeBusinessFacade estimationModeBusinessFacade,
        MiscellaneousApplicationsStatsModeBusinessFacade statsModeBusinessFacade,
        MiscellaneousApplicationsStreamModeBusinessFacade streamModeBusinessFacade,
        MiscellaneousApplicationsWriteModeBusinessFacade writeModeBusinessFacade,
        UserSpecificConfigurationParser configurationParser
    ) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.stubs = stubs;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.statsModeBusinessFacade = statsModeBusinessFacade;
        this.streamModeBusinessFacade = streamModeBusinessFacade;
        this.writeModeBusinessFacade = writeModeBusinessFacade;

        this.configurationParser = configurationParser;
    }

    public static LocalMiscellaneousProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns,
        UserSpecificConfigurationParser configurationParser
    ) {
        var alphaScalePropertiesMutateStub = new LocalScalePropertiesMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate(),
            procedureReturnColumns,
            AlphaScalePropertiesMutateConfig::of
        );
        var collapsePathMutateStub = new LocalCollapsePathMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate()
        );
        var indexInverseMutateStub = new LocalIndexInverseMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate()
        );
        var scalePropertiesMutateStub = new LocalScalePropertiesMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate(),
            procedureReturnColumns,
            ScalePropertiesMutateConfig::of
        );
        var toUndirectedMutateStub = new LocalToUndirectedMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate()
        );

        var stubs =  new MiscellaneousStubs(
            collapsePathMutateStub,
            indexInverseMutateStub,
            alphaScalePropertiesMutateStub,
            scalePropertiesMutateStub,
            toUndirectedMutateStub
        );

        return new LocalMiscellaneousProcedureFacade(
            procedureReturnColumns,
            stubs,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().stats(),
            applicationsFacade.miscellaneous().stream(),
            applicationsFacade.miscellaneous().write(),
            configurationParser
        );
    }


    @Override
    public MiscellaneousStubs miscellaneousStubs() {
        return stubs;
    }

    @Override
    public Stream<ScalePropertiesStreamResult> alphaScalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ScalePropertiesResultBuilderForStreamMode();

        return streamModeBusinessFacade.scaleProperties(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, AlphaScalePropertiesStreamConfig::of),
            resultBuilder
        );
    }

    @Override
    public Stream<ScalePropertiesMutateResult> alphaScalePropertiesMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return  stubs.alphaScaleProperties().execute(graphName,configuration);
    }

    @Override
    public Stream<CollapsePathMutateResult> collapsePathMutate(String graphName, Map<String, Object> configuration) {
        return stubs.collapsePath().execute(graphName,configuration);
    }


    @Override
    public Stream<ScalePropertiesStatsResult> scalePropertiesStats(
        String graphName,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(
            rawConfiguration,
            ScalePropertiesStatsConfig::of
        );

        var shouldDisplayScalerStatistics = procedureReturnColumns.contains("scalerStatistics");
        var resultBuilder = new ScalePropertiesResultBuilderForStatsMode(configuration, shouldDisplayScalerStatistics);

        return statsModeBusinessFacade.scaleProperties(
            GraphName.parse(graphName),
            configuration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> scalePropertiesStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        var result = estimationModeBusinessFacade.scaleProperties(
            configurationParser.parseConfiguration(algorithmConfiguration, ScalePropertiesStatsConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);

    }

    @Override
    public Stream<ScalePropertiesStreamResult> scalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ScalePropertiesResultBuilderForStreamMode();

        return streamModeBusinessFacade.scaleProperties(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, ScalePropertiesStreamConfig::of),
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> scalePropertiesStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.scaleProperties(
            configurationParser.parseConfiguration(algorithmConfiguration, ScalePropertiesStreamConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    @Override
    public Stream<ScalePropertiesMutateResult> scalePropertiesMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return stubs.scaleProperties().execute(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> scalePropertiesMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.scaleProperties().estimate(graphNameOrConfiguration,algorithmConfiguration);
    }

    @Override
    public Stream<ScalePropertiesWriteResult> scalePropertiesWrite(
        String graphName,
        Map<String, Object> configuration
    ) {

        var shouldDisplayScalerStatistics = procedureReturnColumns.contains("scalerStatistics");
        var resultBuilder = new ScalePropertiesResultBuilderForWriteMode(shouldDisplayScalerStatistics);

        return writeModeBusinessFacade.scaleProperties(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, ScalePropertiesWriteConfig::of),
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> scalePropertiesWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.scaleProperties(
            configurationParser.parseConfiguration(algorithmConfiguration, ScalePropertiesWriteConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    @Override
    public Stream<ToUndirectedMutateResult> toUndirected(String graphName, Map<String, Object> configuration) {
        return stubs.toUndirected().execute(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> toUndirectedEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.toUndirected().estimate(graphNameOrConfiguration,algorithmConfiguration);
    }

    @Override
    public Stream<IndexInverseMutateResult> indexInverse(String graphName, Map<String, Object> configuration) {
        return stubs.indexInverse().execute(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> indexInverseEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.indexInverse().estimate(graphNameOrConfiguration,algorithmConfiguration);
    }

}
