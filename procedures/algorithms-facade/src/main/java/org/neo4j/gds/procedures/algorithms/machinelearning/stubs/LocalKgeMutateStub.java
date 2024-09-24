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
package org.neo4j.gds.procedures.algorithms.machinelearning.stubs;

import org.neo4j.gds.algorithms.machinelearning.KGEPredictMutateConfig;
import org.neo4j.gds.applications.algorithms.machinelearning.MachineLearningAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinelearning.MachineLearningAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.machinelearning.KGEMutateResult;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public class LocalKgeMutateStub implements KgeMutateStub {
    private final GenericStub genericStub;
    private final MachineLearningAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade;
    private final MachineLearningAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;

    public LocalKgeMutateStub(
        GenericStub genericStub,
        MachineLearningAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade,
        MachineLearningAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade
    ) {
        this.genericStub = genericStub;
        this.mutateModeBusinessFacade = mutateModeBusinessFacade;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
    }

    @Override
    public KGEPredictMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(KGEPredictMutateConfig::of, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration) {
        return genericStub.getMemoryEstimation(
            configuration,
            KGEPredictMutateConfig::of,
            __ -> estimationModeBusinessFacade.kge()
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration) {
        return genericStub.estimate(
            graphName,
            configuration,
            KGEPredictMutateConfig::of,
            __ -> estimationModeBusinessFacade.kge()
        );
    }

    @Override
    public Stream<KGEMutateResult> execute(String graphNameAsString, Map<String, Object> rawConfiguration) {
        var resultBuilder = new KgeResultBuilderForMutateMode();

        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            KGEPredictMutateConfig::of,
            (graphName, configuration, __) -> mutateModeBusinessFacade.kge(
                graphName,
                configuration,
                resultBuilder
            ),
            resultBuilder
        );
    }


}
