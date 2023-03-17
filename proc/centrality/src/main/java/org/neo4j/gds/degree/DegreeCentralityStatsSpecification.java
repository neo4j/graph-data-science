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
package org.neo4j.gds.degree;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.degree.DegreeCentrality.DEGREE_CENTRALITY_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.degree.stats", description = DEGREE_CENTRALITY_DESCRIPTION, executionMode = STATS)
public class DegreeCentralityStatsSpecification implements AlgorithmSpec<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStatsConfig, Stream<StatsResult>, DegreeCentralityFactory<DegreeCentralityStatsConfig>> {
    @Override
    public String name() {
        return "DegreeCentralityStats";
    }

    @Override
    public DegreeCentralityFactory<DegreeCentralityStatsConfig> algorithmFactory() {
        return new DegreeCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<DegreeCentralityStatsConfig> newConfigFunction() {
        return (__, userInput) -> DegreeCentralityStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStatsConfig, Stream<StatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var builder = new StatsResult.Builder(
                executionContext.returnColumns(),
                computationResult.config().concurrency()
            );

            Optional.ofNullable(computationResult.result())
                .ifPresent(result -> builder.withCentralityFunction(result::get));

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
