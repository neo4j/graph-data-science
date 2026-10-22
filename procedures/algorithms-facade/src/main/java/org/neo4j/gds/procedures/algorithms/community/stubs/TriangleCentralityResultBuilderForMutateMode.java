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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityDistributionComputer;
import org.neo4j.gds.procedures.algorithms.community.TriangleCentralityMutateResult;
import org.neo4j.gds.triangle.TriangleCentralityMutateConfig;
import org.neo4j.gds.triangle.TriangleCentralityResult;

import java.util.Optional;

public class TriangleCentralityResultBuilderForMutateMode implements ResultBuilder<TriangleCentralityMutateConfig, TriangleCentralityResult, TriangleCentralityMutateResult, NodePropertiesWritten> {
    private final CentralityDistributionComputer distributionComputer = new CentralityDistributionComputer();
    private final boolean shouldComputeCentralityDistribution;

    public TriangleCentralityResultBuilderForMutateMode(boolean shouldComputeCentralityDistribution) {
        this.shouldComputeCentralityDistribution = shouldComputeCentralityDistribution;
    }

    @Override
    public TriangleCentralityMutateResult build(
        Graph graph,
        TriangleCentralityMutateConfig configuration,
        Optional<TriangleCentralityResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return TriangleCentralityMutateResult.emptyFrom(timings, configuration.toMap());

        var triangleCentralityResult = result.get();

        var distribution = distributionComputer.compute(
            graph,
            triangleCentralityResult.centralities()::get,
            configuration,
            shouldComputeCentralityDistribution
        );

        return new TriangleCentralityMutateResult(
            triangleCentralityResult.globalTriangleCount(),
            triangleCentralityResult.centralities().size(),
            distribution.centralitySummary(),
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.sideEffectMillis,
            metadata.orElseThrow().value(),
            configuration.toMap(),
            distribution.computeMillis()
        );
    }
}
