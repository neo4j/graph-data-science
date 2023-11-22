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
package org.neo4j.gds.harmonic;

import org.neo4j.gds.StatsComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.procedures.centrality.CentralityStatsResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.harmonic.HarmonicCentralityCompanion.DESCRIPTION;

@GdsCallable(name = "gds.closeness.harmonic.stats", description = DESCRIPTION, executionMode = STATS)
public class HarmonicCentralityStatsSpec implements AlgorithmSpec<HarmonicCentrality, HarmonicResult, HarmonicCentralityStatsConfig, Stream<CentralityStatsResult>, HarmonicCentralityAlgorithmFactory<HarmonicCentralityStatsConfig>> {

    @Override
    public String name() {
        return "HarmonicCentralityStream";
    }

    @Override
    public HarmonicCentralityAlgorithmFactory<HarmonicCentralityStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new HarmonicCentralityAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<HarmonicCentralityStatsConfig> newConfigFunction() {
        return (___,config) -> HarmonicCentralityStatsConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<HarmonicCentrality, HarmonicResult, HarmonicCentralityStatsConfig, Stream<CentralityStatsResult>> computationResultConsumer() {
        return new StatsComputationResultConsumer<>(this::resultBuilder);
    }

    private AbstractResultBuilder<CentralityStatsResult> resultBuilder(
        ComputationResult<HarmonicCentrality, HarmonicResult, HarmonicCentralityStatsConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new CentralityStatsResult.Builder(
            executionContext.returnColumns(),
            computationResult.config().concurrency()
        );

        computationResult.result()
            .ifPresent(result -> builder.withCentralityFunction(result.centralityScoreProvider()));

        return builder;
    }
}
