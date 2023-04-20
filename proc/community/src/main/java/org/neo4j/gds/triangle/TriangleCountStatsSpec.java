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
package org.neo4j.gds.triangle;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.AlgoBaseProc.STATS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.triangleCount.stats", description = STATS_DESCRIPTION, executionMode = STATS)
public class TriangleCountStatsSpec implements AlgorithmSpec<IntersectingTriangleCount, TriangleCountResult, TriangleCountStatsConfig, Stream<TriangleCountStatsResult>, IntersectingTriangleCountFactory<TriangleCountStatsConfig>> {

    @Override
    public String name() {
        return "TriangleCountStats";
    }

    @Override
    public IntersectingTriangleCountFactory<TriangleCountStatsConfig> algorithmFactory() {
        return new IntersectingTriangleCountFactory<>();
    }

    @Override
    public NewConfigFunction<TriangleCountStatsConfig> newConfigFunction() {
        return (___,config) ->  TriangleCountStatsConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<IntersectingTriangleCount, TriangleCountResult, TriangleCountStatsConfig, Stream<TriangleCountStatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var builder = new TriangleCountStatsResult.Builder();
            computationResult.result()
                .ifPresent(result ->
                    builder
                        .withGlobalTriangleCount(result.globalTriangles())
                );

            builder
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(computationResult.config());

            return Stream.of(builder.build());
        };
    }
}
