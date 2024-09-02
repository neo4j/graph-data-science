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
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplicationsWriteModeBusinessFacade;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.CollapsePathMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.IndexInverseMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.ScalePropertiesMutateStub;
import org.neo4j.gds.procedures.algorithms.miscellaneous.stubs.ToUndirectedMutateStub;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.scaleproperties.AlphaScalePropertiesMutateConfig;
import org.neo4j.gds.scaleproperties.AlphaScalePropertiesStreamConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesMutateConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesStatsConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesStreamConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesWriteConfig;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public final class MiscellaneousProcedureFacade {
    private final ProcedureReturnColumns procedureReturnColumns;

    //stubs
    private final ScalePropertiesMutateStub alphaScalePropertiesMutateStub;
    private final CollapsePathMutateStub collapsePathMutateStub;
    private final IndexInverseMutateStub indexInverseMutateStub;
    private final ScalePropertiesMutateStub scalePropertiesMutateStub;
    private final ToUndirectedMutateStub toUndirectedMutateStub;

    //
    private final MiscellaneousApplicationsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final MiscellaneousApplicationsStatsModeBusinessFacade statsModeBusinessFacade;
    private final MiscellaneousApplicationsStreamModeBusinessFacade streamModeBusinessFacade;
    private final MiscellaneousApplicationsWriteModeBusinessFacade writeModeBusinessFacade;

    private final ConfigurationParser configurationParser;
    private final User user;


    private MiscellaneousProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        ScalePropertiesMutateStub alphaScalePropertiesMutateStub,
        CollapsePathMutateStub collapsePathMutateStub,
        IndexInverseMutateStub indexInverseMutateStub,
        ScalePropertiesMutateStub scalePropertiesMutateStub,
        ToUndirectedMutateStub toUndirectedMutateStub,
        MiscellaneousApplicationsEstimationModeBusinessFacade estimationModeBusinessFacade,
        MiscellaneousApplicationsStatsModeBusinessFacade statsModeBusinessFacade,
        MiscellaneousApplicationsStreamModeBusinessFacade streamModeBusinessFacade,
        MiscellaneousApplicationsWriteModeBusinessFacade writeModeBusinessFacade,
        ConfigurationParser configurationParser,
        User user
    ) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.alphaScalePropertiesMutateStub = alphaScalePropertiesMutateStub;
        this.collapsePathMutateStub = collapsePathMutateStub;
        this.indexInverseMutateStub = indexInverseMutateStub;
        this.scalePropertiesMutateStub = scalePropertiesMutateStub;
        this.toUndirectedMutateStub = toUndirectedMutateStub;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.statsModeBusinessFacade = statsModeBusinessFacade;
        this.streamModeBusinessFacade = streamModeBusinessFacade;
        this.writeModeBusinessFacade = writeModeBusinessFacade;

        this.configurationParser = configurationParser;
        this.user = user;
    }

    public static MiscellaneousProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns,
        ConfigurationParser configurationParser,
         User user
    ) {
        var alphaScalePropertiesMutateStub = new ScalePropertiesMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate(),
            procedureReturnColumns,
            AlphaScalePropertiesMutateConfig::of
        );
        var collapsePathMutateStub = new CollapsePathMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate()
        );
        var indexInverseMutateStub = new IndexInverseMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate()
        );
        var scalePropertiesMutateStub = new ScalePropertiesMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate(),
            procedureReturnColumns,
            ScalePropertiesMutateConfig::of
        );
        var toUndirectedMutateStub = new ToUndirectedMutateStub(
            genericStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().mutate()
        );

        return new MiscellaneousProcedureFacade(
            procedureReturnColumns,
            alphaScalePropertiesMutateStub,
            collapsePathMutateStub,
            indexInverseMutateStub,
            scalePropertiesMutateStub,
            toUndirectedMutateStub,
            applicationsFacade.miscellaneous().estimate(),
            applicationsFacade.miscellaneous().stats(),
            applicationsFacade.miscellaneous().stream(),
            applicationsFacade.miscellaneous().write(),
            configurationParser,
            user
        );
    }

    public ScalePropertiesMutateStub alphaScalePropertiesMutateStub() {
        return alphaScalePropertiesMutateStub;
    }

    public Stream<ScalePropertiesStreamResult> alphaScalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ScalePropertiesResultBuilderForStreamMode();

        return streamModeBusinessFacade.scaleProperties(
            GraphName.parse(graphName),
            parseConfiguration(configuration, AlphaScalePropertiesStreamConfig::of),
            resultBuilder
        );
    }

    public CollapsePathMutateStub collapsePathMutateStub() {
        return collapsePathMutateStub;
    }

    public IndexInverseMutateStub indexInverseMutateStub() {
        return indexInverseMutateStub;
    }

    public ScalePropertiesMutateStub scalePropertiesMutateStub() {
        return scalePropertiesMutateStub;
    }

    public Stream<ScalePropertiesStatsResult> scalePropertiesStats(
        String graphName,
        Map<String, Object> configuration
    ) {

        var shouldDisplayScalerStatistics = procedureReturnColumns.contains("scalerStatistics");
        var resultBuilder = new ScalePropertiesResultBuilderForStatsMode(shouldDisplayScalerStatistics);

        return statsModeBusinessFacade.scaleProperties(
            GraphName.parse(graphName),
            parseConfiguration(configuration, ScalePropertiesStatsConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> scalePropertiesStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        var result = estimationModeBusinessFacade.scaleProperties(
            parseConfiguration(algorithmConfiguration, ScalePropertiesStatsConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);

    }

    public Stream<ScalePropertiesStreamResult> scalePropertiesStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ScalePropertiesResultBuilderForStreamMode();

        return streamModeBusinessFacade.scaleProperties(
            GraphName.parse(graphName),
            parseConfiguration(configuration, ScalePropertiesStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> scalePropertiesStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.scaleProperties(
            parseConfiguration(algorithmConfiguration, ScalePropertiesStreamConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    public Stream<ScalePropertiesWriteResult> scalePropertiesWrite(
        String graphName,
        Map<String, Object> configuration
    ) {

        var shouldDisplayScalerStatistics = procedureReturnColumns.contains("scalerStatistics");
        var resultBuilder = new ScalePropertiesResultBuilderForWriteMode(shouldDisplayScalerStatistics);

        return writeModeBusinessFacade.scaleProperties(
            GraphName.parse(graphName),
            parseConfiguration(configuration, ScalePropertiesWriteConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> scalePropertiesWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.scaleProperties(
            parseConfiguration(algorithmConfiguration, ScalePropertiesWriteConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    public ToUndirectedMutateStub toUndirectedMutateStub() {
        return toUndirectedMutateStub;
    }

    private <C extends AlgoBaseConfig> C parseConfiguration(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configurationMapper
    ) {
        return configurationParser.parseConfiguration(
            configuration,
            configurationMapper,
            user
        );
    }

}
