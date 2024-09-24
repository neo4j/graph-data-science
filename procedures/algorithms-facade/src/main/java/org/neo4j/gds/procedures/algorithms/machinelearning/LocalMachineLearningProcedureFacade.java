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
package org.neo4j.gds.procedures.algorithms.machinelearning;

import org.neo4j.gds.algorithms.machinelearning.KGEPredictStreamConfig;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictWriteConfig;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinelearning.MachineLearningAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinelearning.MachineLearningAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.machinelearning.stubs.KgeMutateStub;
import org.neo4j.gds.procedures.algorithms.machinelearning.stubs.LocalKgeMutateStub;
import org.neo4j.gds.procedures.algorithms.machinelearning.stubs.LocalSplitRelationshipsMutateStub;
import org.neo4j.gds.procedures.algorithms.machinelearning.stubs.SplitRelationshipsMutateStub;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public final class LocalMachineLearningProcedureFacade implements MachineLearningProcedureFacade {

    private final MachineLearningAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade;
    private final MachineLearningAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade;

    private final KgeMutateStub kgeMutateStub;
    private final SplitRelationshipsMutateStub splitRelationshipsMutateStub;

    private final UserSpecificConfigurationParser configurationParser;

    private LocalMachineLearningProcedureFacade(
        KgeMutateStub kgeMutateStub,
        SplitRelationshipsMutateStub splitRelationshipsMutateStub,
        MachineLearningAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade,
        MachineLearningAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade,
        UserSpecificConfigurationParser configurationParser
    ) {
        this.streamModeBusinessFacade = streamModeBusinessFacade;
        this.writeModeBusinessFacade = writeModeBusinessFacade;
        this.kgeMutateStub = kgeMutateStub;
        this.splitRelationshipsMutateStub = splitRelationshipsMutateStub;
        this.configurationParser = configurationParser;
    }

    public static MachineLearningProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        UserSpecificConfigurationParser configurationParser
    ) {
        var kgeMutateStub = new LocalKgeMutateStub(
            genericStub,
            applicationsFacade.machineLearning().mutate(),
            applicationsFacade.machineLearning().estimate()
        );
        var splitRelationshipsMutateStub = new LocalSplitRelationshipsMutateStub(
            genericStub,
            applicationsFacade.machineLearning().mutate(),
            applicationsFacade.machineLearning().estimate()
        );

        return new LocalMachineLearningProcedureFacade(
            kgeMutateStub,
            splitRelationshipsMutateStub,
            applicationsFacade.machineLearning().stream(),
            applicationsFacade.machineLearning().write(),
            configurationParser
        );
    }

    @Override
    public KgeMutateStub kgeMutateStub() {
        return kgeMutateStub;
    }

    @Override
    public Stream<KGEStreamResult> kgeStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new KgeResultBuilderForStreamMode();

        return streamModeBusinessFacade.kge(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, KGEPredictStreamConfig::of),
            resultBuilder
        );
    }

    @Override
    public Stream<KGEWriteResult> kgeWrite(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new KgeResultBuilderForWriteMode();

        return writeModeBusinessFacade.kge(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, KGEPredictWriteConfig::of),
            resultBuilder
        );
    }

    @Override
    public SplitRelationshipsMutateStub splitRelationshipsMutateStub() {
        return splitRelationshipsMutateStub;
    }


}
