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
package org.neo4j.gds.randomwalk;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.pathfinding.RandomWalkStreamResult;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkAlgorithmFactory;
import org.neo4j.gds.traversal.RandomWalkStreamConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;


@GdsCallable(name = "gds.randomWalk.stream", description = Constants.RANDOM_WALK_DESCRIPTION, executionMode= STREAM)
public class RandomWalkStreamSpec implements AlgorithmSpec<RandomWalk, Stream<long[]>, RandomWalkStreamConfig, Stream<RandomWalkStreamResult>, RandomWalkAlgorithmFactory<RandomWalkStreamConfig>> {
    @Override
    public String name() {
        return "RandomWalkStream";
    }

    @Override
    public RandomWalkAlgorithmFactory<RandomWalkStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new RandomWalkAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<RandomWalkStreamConfig> newConfigFunction() {
        return (__, config) -> RandomWalkStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<RandomWalk, Stream<long[]>, RandomWalkStreamConfig, Stream<RandomWalkStreamResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
