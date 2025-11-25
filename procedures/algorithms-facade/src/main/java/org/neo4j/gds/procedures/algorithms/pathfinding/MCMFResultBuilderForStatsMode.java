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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.maxflow.MaxFlowStatsConfig;
import org.neo4j.gds.mcmf.CostFlowResult;

import java.util.Optional;
import java.util.stream.Stream;

class MCMFResultBuilderForStatsMode implements StatsResultBuilder<CostFlowResult, Stream<MCMFStatsResult>> {
    private final MaxFlowStatsConfig configuration;

    MCMFResultBuilderForStatsMode(MaxFlowStatsConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    public Stream<MCMFStatsResult> build(
        Graph graph,
        Optional<CostFlowResult> flowResult,
        AlgorithmProcessingTimings timings
    ) {
        return Stream.of(flowResult.map(flowResult1 -> new MCMFStatsResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            flowResult1.totalFlow(),
            flowResult1.totalCost(),
            configuration.toMap()
        )).orElse(MCMFStatsResult.emptyFrom(timings, configuration.toMap())));
    }
}
