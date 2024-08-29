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
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinelearning.MachineLearningAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinelearning.MachineLearningAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.procedures.algorithms.machinelearning.stubs.KgeMutateStub;
import org.neo4j.gds.procedures.algorithms.runners.AlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public final class MachineLearningProcedureFacade {
    private final ApplicationsFacade applicationsFacade;

    private final KgeMutateStub kgeMutateStub;

    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;

    private MachineLearningProcedureFacade(
        ApplicationsFacade applicationsFacade,
        KgeMutateStub kgeMutateStub,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding
    ) {
        this.applicationsFacade = applicationsFacade;
        this.kgeMutateStub = kgeMutateStub;
        this.algorithmExecutionScaffolding = algorithmExecutionScaffolding;
    }

    public static MachineLearningProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding
    ) {
        var kgeMutateStub = new KgeMutateStub(genericStub, applicationsFacade);

        return new MachineLearningProcedureFacade(applicationsFacade, kgeMutateStub, algorithmExecutionScaffolding);
    }

    public KgeMutateStub kgeMutateStub() {
        return kgeMutateStub;
    }

    public Stream<KGEStreamResult> kgeStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new KgeResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            KGEPredictStreamConfig::of,
            streamMode()::kge,
            resultBuilder
        );
    }

    public Stream<KGEWriteResult> kgeWrite(String graphNameAsString, Map<String, Object> rawConfiguration) {
        var resultBuilder = new KgeResultBuilderForWriteMode();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphNameAsString,
            rawConfiguration,
            KGEPredictWriteConfig::of,
            (graphName, configuration, __) -> writeMode().kge(
                graphName,
                configuration,
                resultBuilder
            ),
            resultBuilder
        );
    }

    private MachineLearningAlgorithmsStreamModeBusinessFacade streamMode() {
        return applicationsFacade.machineLearning().stream();
    }

    private MachineLearningAlgorithmsWriteModeBusinessFacade writeMode() {
        return applicationsFacade.machineLearning().write();
    }
}
