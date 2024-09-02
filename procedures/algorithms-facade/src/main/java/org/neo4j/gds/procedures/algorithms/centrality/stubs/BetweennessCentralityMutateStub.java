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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityMutateResult;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.procedures.algorithms.stubs.MutateStub;

import java.util.Map;
import java.util.stream.Stream;

public class BetweennessCentralityMutateStub implements MutateStub<BetweennessCentralityMutateConfig, CentralityMutateResult> {
    private final GenericStub genericStub;
    private final CentralityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade;
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final ProcedureReturnColumns procedureReturnColumns;

    public BetweennessCentralityMutateStub(
        GenericStub genericStub,
        CentralityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        ProcedureReturnColumns procedureReturnColumns
    ) {
        this.genericStub = genericStub;
        this.mutateModeBusinessFacade = mutateModeBusinessFacade;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.procedureReturnColumns = procedureReturnColumns;
    }

    @Override
    public BetweennessCentralityMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(BetweennessCentralityMutateConfig::of, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration) {
        return genericStub.getMemoryEstimation(
            configuration,
            BetweennessCentralityMutateConfig::of,
            estimationModeBusinessFacade::betweennessCentrality
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration) {
        return genericStub.estimate(
            graphName,
            configuration,
            BetweennessCentralityMutateConfig::of,
            estimationModeBusinessFacade::betweennessCentrality
        );
    }

    @Override
    public Stream<CentralityMutateResult> execute(String graphNameAsString, Map<String, Object> rawConfiguration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new BetweennessCentralityResultBuilderForMutateMode(shouldComputeCentralityDistribution);

        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            BetweennessCentralityMutateConfig::of,
            mutateModeBusinessFacade::betweennessCentrality,
            resultBuilder
        );
    }

}
