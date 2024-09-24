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
package org.neo4j.gds.procedures.algorithms.embeddings.stubs;

import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateConfig;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.embeddings.DefaultNodeEmbeddingMutateResult;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public class LocalFastRPMutateStub implements FastRPMutateStub {

    private final GenericStub genericStub;
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final NodeEmbeddingAlgorithmsMutateModeBusinessFacade  mutateModeBusinessFacade;

    public LocalFastRPMutateStub(
        GenericStub genericStub,
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        NodeEmbeddingAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade
    ) {
        this.genericStub = genericStub;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.mutateModeBusinessFacade = mutateModeBusinessFacade;
    }

    @Override
    public FastRPMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(FastRPMutateConfig::of, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> rawConfiguration) {
        return genericStub.getMemoryEstimation(
            rawConfiguration,
            FastRPMutateConfig::of,
            estimationModeBusinessFacade::fastRP
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphNameAsString, Map<String, Object> rawConfiguration) {
        return genericStub.estimate(
            graphNameAsString,
            rawConfiguration,
            FastRPMutateConfig::of,
            estimationModeBusinessFacade::fastRP
        );
    }

    @Override
    public Stream<DefaultNodeEmbeddingMutateResult> execute(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new FastRPResultBuilderForMutateMode();

        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            FastRPMutateConfig::of,
            mutateModeBusinessFacade::fastRP,
            resultBuilder
        );
    }


}
