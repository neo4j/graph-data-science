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
package org.neo4j.gds.betweenness;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.centrality.CentralityStatsResult;

import java.util.stream.Stream;

import static org.neo4j.gds.betweenness.BetweennessCentrality.BETWEENNESS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.betweenness.stats", description = BETWEENNESS_DESCRIPTION, executionMode = STATS)
public class BetweennessCentralityStatsSpecification implements AlgorithmSpec<BetweennessCentrality, BetwennessCentralityResult, BetweennessCentralityStatsConfig, Stream<CentralityStatsResult>, BetweennessCentralityFactory<BetweennessCentralityStatsConfig>> {
    @Override
    public String name() {
        return "BetweennessCentralityStats";
    }

    @Override
    public BetweennessCentralityFactory<BetweennessCentralityStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new BetweennessCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<BetweennessCentralityStatsConfig> newConfigFunction() {
        return (__, userInput) -> BetweennessCentralityStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<BetweennessCentrality, BetwennessCentralityResult, BetweennessCentralityStatsConfig, Stream<CentralityStatsResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
