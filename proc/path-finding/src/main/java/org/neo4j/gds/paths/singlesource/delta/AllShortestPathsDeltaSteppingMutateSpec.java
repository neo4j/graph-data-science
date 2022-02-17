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
package org.neo4j.gds.paths.singlesource.delta;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.MutateResult;
import org.neo4j.gds.paths.ShortestPathMutateResultConsumer;
import org.neo4j.gds.paths.delta.DeltaStepping;
import org.neo4j.gds.paths.delta.DeltaSteppingFactory;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaMutateConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(name = "gds.allShortestPaths.delta.mutate", description = AllShortestPathsDeltaSteppingMutateSpec.DELTA_DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class AllShortestPathsDeltaSteppingMutateSpec implements AlgorithmSpec<DeltaStepping, DijkstraResult, AllShortestPathsDeltaMutateConfig, Stream<MutateResult>, DeltaSteppingFactory<AllShortestPathsDeltaMutateConfig>> {
    static final String DELTA_DESCRIPTION = "The Delta Stepping shortest path algorithm computes the shortest (weighted) path between one node and any other node in the graph.";

    @Override
    public String name() {
        return "gds.allShortestPaths.delta.mutate";
    }

    @Override
    public DeltaSteppingFactory<AllShortestPathsDeltaMutateConfig> algorithmFactory() {
        return new DeltaSteppingFactory<>();
    }

    @Override
    public NewConfigFunction<AllShortestPathsDeltaMutateConfig> newConfigFunction() {
        return (username, configuration) -> AllShortestPathsDeltaMutateConfig.of(configuration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ComputationResultConsumer<DeltaStepping, DijkstraResult, AllShortestPathsDeltaMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new ShortestPathMutateResultConsumer<>();
    }
}
