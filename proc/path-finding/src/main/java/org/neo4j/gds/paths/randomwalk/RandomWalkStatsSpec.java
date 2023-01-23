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
package org.neo4j.gds.paths.randomwalk;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.results.StandardModeResult;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkAlgorithmFactory;
import org.neo4j.gds.traversal.RandomWalkStatsConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.paths.randomwalk.RandomWalkStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.randomWalk.stats", description = DESCRIPTION, executionMode = STATS)
public class RandomWalkStatsSpec implements AlgorithmSpec<RandomWalk, Stream<long[]>, RandomWalkStatsConfig, Stream<StandardModeResult>, RandomWalkAlgorithmFactory<RandomWalkStatsConfig>> {

    @Override
    public String name() {
        return "RandomWalkStats";
    }

    @Override
    public RandomWalkAlgorithmFactory<RandomWalkStatsConfig> algorithmFactory() {
        return new RandomWalkAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<RandomWalkStatsConfig> newConfigFunction() {
        return (__, userInput) -> RandomWalkStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<RandomWalk, Stream<long[]>, RandomWalkStatsConfig, Stream<StandardModeResult>> computationResultConsumer() {
        return (computationResult, executionContext) ->
            Stream.of(new StandardModeResult(
                computationResult.preProcessingMillis(),
                computationResult.computeMillis(),
                computationResult.config().toMap()
            ));
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
