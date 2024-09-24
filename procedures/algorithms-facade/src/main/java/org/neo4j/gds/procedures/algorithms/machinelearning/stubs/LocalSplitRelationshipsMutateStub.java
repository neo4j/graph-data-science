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

import org.neo4j.gds.applications.algorithms.machinelearning.MachineLearningAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinelearning.MachineLearningAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateConfig;
import org.neo4j.gds.procedures.algorithms.machinelearning.SplitRelationshipsMutateResult;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public class LocalSplitRelationshipsMutateStub implements SplitRelationshipsMutateStub {
    private final GenericStub genericStub;
    private final MachineLearningAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade;
    private final MachineLearningAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;

    public LocalSplitRelationshipsMutateStub(
        GenericStub genericStub,
        MachineLearningAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade,
        MachineLearningAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade
    ) {
        this.genericStub = genericStub;
        this.mutateModeBusinessFacade = mutateModeBusinessFacade;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
    }

    @Override
    public SplitRelationshipsMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(SplitRelationshipsMutateConfig::of, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> rawConfiguration) {
        return genericStub.getMemoryEstimation(
            rawConfiguration,
            SplitRelationshipsMutateConfig::of,
            estimationModeBusinessFacade::splitRelationships
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> rawConfiguration) {
        return genericStub.estimate(
            graphName,
            rawConfiguration,
            SplitRelationshipsMutateConfig::of,
            estimationModeBusinessFacade::splitRelationships
        );
    }

    @Override
    public Stream<SplitRelationshipsMutateResult> execute(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new SplitRelationshipsResultBuilderForMutateMode();

        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            SplitRelationshipsMutateConfig::of,
            (graphName, configuration, __) -> mutateModeBusinessFacade.splitRelationships(
                graphName,
                configuration,
                resultBuilder
            ),
            resultBuilder
        );
    }


}
