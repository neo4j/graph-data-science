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
package org.neo4j.gds.procedures.algorithms.community.stubs;

import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.hdbscan.HDBScanMutateConfig;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.community.HDBScanMutateResult;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public class LocalHDBScanMutateStub implements HDBScanMutateStub {

    private final GenericStub genericStub;
    private final CommunityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade;
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;

    public LocalHDBScanMutateStub(
        GenericStub genericStub,
        CommunityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade,
        CommunityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade
    ) {
        this.genericStub = genericStub;
        this.mutateModeBusinessFacade = mutateModeBusinessFacade;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
    }


    @Override
    public HDBScanMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(HDBScanMutateConfig::of, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration) {
        return genericStub.getMemoryEstimation(
            configuration,
            HDBScanMutateConfig::of,
            estimationModeBusinessFacade::hdbscan
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration) {
        return genericStub.estimate(
            graphName,
            configuration,
            HDBScanMutateConfig::of,
            estimationModeBusinessFacade::hdbscan
        );
    }

    @Override
    public Stream<HDBScanMutateResult> execute(String graphNameAsString, Map<String, Object> rawConfiguration) {
        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            HDBScanMutateConfig::of,
            mutateModeBusinessFacade::hdbscan,
            new HDBScanResultBuilderForMutateMode()
        );
    }
}
