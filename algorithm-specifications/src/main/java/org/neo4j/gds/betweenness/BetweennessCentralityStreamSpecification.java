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
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityStreamResult;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;

import java.util.stream.Stream;

@GdsCallable(name = "gds.betweenness.stream", description = Constants.BETWEENNESS_DESCRIPTION, executionMode = ExecutionMode.STREAM)
public class BetweennessCentralityStreamSpecification implements AlgorithmSpec<BetweennessCentrality, BetwennessCentralityResult, BetweennessCentralityStreamConfig, Stream<CentralityStreamResult>, BetweennessCentralityFactory<BetweennessCentralityStreamConfig>> {

    @Override
    public String name() {
        return "BetweennessCentralityStream";
    }

    @Override
    public BetweennessCentralityFactory<BetweennessCentralityStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new BetweennessCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<BetweennessCentralityStreamConfig> newConfigFunction() {
        return (__, userInput) -> BetweennessCentralityStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<BetweennessCentrality, BetwennessCentralityResult, BetweennessCentralityStreamConfig, Stream<CentralityStreamResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
