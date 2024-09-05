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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.procedures.algorithms.centrality.BetaClosenessCentralityMutateResult;

import java.util.Optional;

class BetaClosenessCentralityResultBuilderForMutateMode implements ResultBuilder<ClosenessCentralityMutateConfig, ClosenessCentralityResult, BetaClosenessCentralityMutateResult, NodePropertiesWritten> {
    private final GenericCentralityResultBuilderForMutateMode genericResultBuilder = new GenericCentralityResultBuilderForMutateMode();

    private final boolean shouldComputeCentralityDistribution;

    BetaClosenessCentralityResultBuilderForMutateMode(boolean shouldComputeCentralityDistribution) {
        this.shouldComputeCentralityDistribution = shouldComputeCentralityDistribution;
    }

    @Override
    public BetaClosenessCentralityMutateResult build(
        Graph graph,
        ClosenessCentralityMutateConfig configuration,
        Optional<ClosenessCentralityResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        var centralityMutateResult = genericResultBuilder.build(
            graph,
            configuration,
            result,
            timings,
            metadata,
            shouldComputeCentralityDistribution
        );

        return new BetaClosenessCentralityMutateResult(
            centralityMutateResult.nodePropertiesWritten,
            centralityMutateResult.preProcessingMillis,
            centralityMutateResult.computeMillis,
            centralityMutateResult.postProcessingMillis,
            centralityMutateResult.mutateMillis,
            configuration.mutateProperty(),
            centralityMutateResult.centralityDistribution,
            centralityMutateResult.configuration
        );
    }
}
