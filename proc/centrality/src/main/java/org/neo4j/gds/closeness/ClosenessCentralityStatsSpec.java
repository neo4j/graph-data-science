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
package org.neo4j.gds.closeness;

import org.neo4j.gds.StatsComputationResultConsumer;
import org.neo4j.gds.beta.closeness.ClosenessCentrality;
import org.neo4j.gds.beta.closeness.ClosenessCentralityFactory;
import org.neo4j.gds.beta.closeness.ClosenessCentralityResult;
import org.neo4j.gds.beta.closeness.ClosenessCentralityStatsConfig;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.beta.closeness.ClosenessCentrality.CLOSENESS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.beta.closeness.stats", description = CLOSENESS_DESCRIPTION, executionMode = STATS)
public class ClosenessCentralityStatsSpec implements AlgorithmSpec<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityStatsConfig, Stream<StatsResult>, ClosenessCentralityFactory<ClosenessCentralityStatsConfig>> {

    @Override
    public String name() {
        return "ClosenessCentralityStream";
    }

    @Override
    public ClosenessCentralityFactory<ClosenessCentralityStatsConfig> algorithmFactory() {
        return new ClosenessCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<ClosenessCentralityStatsConfig> newConfigFunction() {
        return (___, config) -> ClosenessCentralityStatsConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityStatsConfig, Stream<StatsResult>> computationResultConsumer() {
        return new StatsComputationResultConsumer(this::resultBuilder);

    }


    private AbstractCentralityResultBuilder<StatsResult> resultBuilder(
        ComputationResult<ClosenessCentrality, ClosenessCentralityResult,
            ClosenessCentralityStatsConfig> computationResult,
        ExecutionContext executionContext
    ) {

        var centralities = computationResult
            .result()
            .centralities();

        AbstractCentralityResultBuilder<StatsResult> builder = new StatsResult.Builder(
            executionContext.returnColumns(),
            computationResult.config().concurrency()
        );

        Optional.ofNullable(computationResult.result())
            .ifPresent(result -> builder.withCentralityFunction(centralities::get));

        return builder;

    }
}
