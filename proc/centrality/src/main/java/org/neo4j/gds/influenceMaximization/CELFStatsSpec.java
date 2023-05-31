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
package org.neo4j.gds.influenceMaximization;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.influenceMaximization.CELFStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.beta.influenceMaximization.celf.stats", description = DESCRIPTION, executionMode = STATS)
public class CELFStatsSpec implements AlgorithmSpec<CELF, LongDoubleScatterMap, InfluenceMaximizationStatsConfig, Stream<StatsResult>, CELFAlgorithmFactory<InfluenceMaximizationStatsConfig>> {
    @Override
    public String name() {
        return "CELFStats";
    }

    @Override
    public CELFAlgorithmFactory<InfluenceMaximizationStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new CELFAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<InfluenceMaximizationStatsConfig> newConfigFunction() {
        return (__, userInput) -> InfluenceMaximizationStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<CELF, LongDoubleScatterMap, InfluenceMaximizationStatsConfig, Stream<StatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var celfSpreadSet = computationResult.result();
            if (celfSpreadSet.isEmpty()) {
                return Stream.empty();
            }

            var statsBuilder = StatsResult.builder();

            var celfSpreadSetValues = celfSpreadSet.get().values;
            var statsResult = statsBuilder
                .withTotalSpread(Arrays.stream(celfSpreadSetValues).sum())
                .withNodeCount(computationResult.graph().nodeCount())
                .withComputeMillis(computationResult.computeMillis())
                .withConfig(computationResult.config())
                .build();

            return Stream.of(statsResult);
        };

    }
}
