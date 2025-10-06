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
package org.neo4j.gds.procedures.algorithms.pathfinding.stats;

import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.MaxFlowStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

class MaxFlowStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<FlowResult>, Stream<MaxFlowStatsResult>> {

    private final Map<String, Object> configuration;

    MaxFlowStatsResultTransformer(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public Stream<MaxFlowStatsResult> apply(TimedAlgorithmResult<FlowResult> algorithmResult) {
        var spanningTree = algorithmResult.result();
        var statsResult = new MaxFlowStatsResult(
            0,
            algorithmResult.computeMillis(),
            spanningTree.totalFlow(),
            configuration
        );
        return Stream.of(statsResult);
    }
}
