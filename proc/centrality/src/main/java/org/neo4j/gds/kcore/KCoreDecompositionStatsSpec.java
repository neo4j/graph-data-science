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
package org.neo4j.gds.kcore;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.kcore.KCoreDecomposition.KCORE_DESCRIPTION;

@GdsCallable(name = "gds.kcore.stats", description = KCORE_DESCRIPTION, executionMode = STATS)
public class KCoreDecompositionStatsSpec implements AlgorithmSpec<KCoreDecomposition, KCoreDecompositionResult, KCoreDecompositionStatsConfig, Stream<StatsResult>, KCoreDecompositionAlgorithmFactory<KCoreDecompositionStatsConfig>> {
    @Override
    public String name() {
        return "KCoreMutate";
    }

    @Override
    public KCoreDecompositionAlgorithmFactory<KCoreDecompositionStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new KCoreDecompositionAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KCoreDecompositionStatsConfig> newConfigFunction() {
        return (__, userInput) -> KCoreDecompositionStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<KCoreDecomposition, KCoreDecompositionResult, KCoreDecompositionStatsConfig, Stream<StatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {

            var builder = new StatsResult.Builder();

            computationResult.result().ifPresent(result -> builder.withDegeneracy(result.degeneracy()));

            return Stream.of(
                builder.withPreProcessingMillis(computationResult.preProcessingMillis())
                    .withComputeMillis(computationResult.computeMillis())
                    .withNodeCount(computationResult.graph().nodeCount())
                    .withConfig(computationResult.config())
                    .build()
            );
        };
    }


}
