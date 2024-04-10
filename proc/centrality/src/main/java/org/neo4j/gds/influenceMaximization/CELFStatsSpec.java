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

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.centrality.celf.CELFStatsResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.influenceMaximization.CELFStreamProc.DESCRIPTION;

@GdsCallable(
    name = "gds.influenceMaximization.celf.stats",
    aliases = {"gds.beta.influenceMaximization.celf.stats"},
    description = DESCRIPTION,
    executionMode = STATS
)
public class CELFStatsSpec implements AlgorithmSpec<CELF, CELFResult, InfluenceMaximizationStatsConfig, Stream<CELFStatsResult>, CELFAlgorithmFactory<InfluenceMaximizationStatsConfig>> {
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
    public ComputationResultConsumer<CELF, CELFResult, InfluenceMaximizationStatsConfig, Stream<CELFStatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var celfResult = computationResult.result();
            if (celfResult.isEmpty()) {
                return Stream.empty();
            }

            var statsBuilder = CELFStatsResult.builder();

            var statsResult = statsBuilder
                .withTotalSpread(celfResult.map(res -> res.totalSpread()).orElse(0D))
                .withNodeCount(computationResult.graph().nodeCount())
                .withComputeMillis(computationResult.computeMillis())
                .withConfig(computationResult.config())
                .build();

            return Stream.of(statsResult);
        };

    }
}
