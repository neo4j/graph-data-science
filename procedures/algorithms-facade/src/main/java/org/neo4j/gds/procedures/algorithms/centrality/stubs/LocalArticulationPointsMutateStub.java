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
package org.neo4j.gds.procedures.algorithms.centrality.stubs;

import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.articulationpoints.ArticulationPointsMutateConfig;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.centrality.ArticulationPointsMutateResult;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public class LocalArticulationPointsMutateStub implements ArticulationPointsMutateStub {
    private final GenericStub genericStub;
    private final CentralityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade;
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;

    public LocalArticulationPointsMutateStub(
        GenericStub genericStub,
        CentralityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade
    ) {
        this.genericStub = genericStub;
        this.mutateModeBusinessFacade = mutateModeBusinessFacade;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
    }

    @Override
    public ArticulationPointsMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(ArticulationPointsMutateConfig::of, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration) {
        return genericStub.getMemoryEstimation(
            configuration,
            ArticulationPointsMutateConfig::of,
            (config) -> estimationModeBusinessFacade.articulationPoints()
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration) {
        return genericStub.estimate(
            graphName,
            configuration,
            ArticulationPointsMutateConfig::of,
            (config) -> estimationModeBusinessFacade.articulationPoints()
        );
    }

    @Override
    public Stream<ArticulationPointsMutateResult> execute(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new ArticulationPointsResultBuilderForMutateMode();

        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            ArticulationPointsMutateConfig::of,
            mutateModeBusinessFacade::articulationPoints,
            resultBuilder
        );
    }

}
