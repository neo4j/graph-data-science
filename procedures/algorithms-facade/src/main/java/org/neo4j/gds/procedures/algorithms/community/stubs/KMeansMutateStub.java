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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.kmeans.KmeansMutateConfig;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.community.KmeansMutateResult;
import org.neo4j.gds.procedures.algorithms.community.ProcedureStatisticsComputationInstructions;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.procedures.algorithms.stubs.MutateStub;

import java.util.Map;
import java.util.stream.Stream;

public class KMeansMutateStub implements MutateStub<KmeansMutateConfig, KmeansMutateResult> {

    private final GenericStub genericStub;
    private final CommunityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade;
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final ProcedureReturnColumns procedureReturnColumns;

    public KMeansMutateStub(
        GenericStub genericStub,
        CommunityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade,
        CommunityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        ProcedureReturnColumns procedureReturnColumns
    ) {
        this.genericStub = genericStub;
        this.mutateModeBusinessFacade = mutateModeBusinessFacade;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.procedureReturnColumns = procedureReturnColumns;
    }

    @Override
    public KmeansMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(KmeansMutateConfig::of, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> rawConfiguration) {
        return genericStub.getMemoryEstimation(
            username,
            rawConfiguration,
            KmeansMutateConfig::of,
            estimationModeBusinessFacade::kMeans
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> rawConfiguration) {
        return genericStub.estimate(
            graphName,
            rawConfiguration,
            KmeansMutateConfig::of,
            estimationModeBusinessFacade::kMeans
        );
    }

    @Override
    public Stream<KmeansMutateResult> execute(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var statisticsComputationInstructions = ProcedureStatisticsComputationInstructions.forCommunities(
            procedureReturnColumns);
        var shouldComputeListOfCentroids = procedureReturnColumns.contains("centroids");
        var resultBuilder = new KMeansResultBuilderForMutateMode(statisticsComputationInstructions, shouldComputeListOfCentroids);

        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            KmeansMutateConfig::of,
            mutateModeBusinessFacade::kMeans,
            resultBuilder
        );
    }

}
