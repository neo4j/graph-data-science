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
package org.neo4j.gds.paths.sourcetarget;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraFactory;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingMutateResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;
import static org.neo4j.gds.paths.sourcetarget.SinglePairShortestPathConstants.DIJKSTRA_DESCRIPTION;

@GdsCallable(name = "gds.shortestPath.dijkstra.mutate", description = DIJKSTRA_DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class ShortestPathDijkstraMutateSpec implements AlgorithmSpec<Dijkstra, PathFindingResult, ShortestPathDijkstraMutateConfig, Stream<PathFindingMutateResult>, DijkstraFactory.SourceTargetDijkstraFactory<ShortestPathDijkstraMutateConfig>> {

    @Override
    public String name() {
        return "DijkstraMutate";
    }

    @Override
    public DijkstraFactory.SourceTargetDijkstraFactory<ShortestPathDijkstraMutateConfig> algorithmFactory(
        ExecutionContext executionContext
    ) {
        return new DijkstraFactory.SourceTargetDijkstraFactory<>();
    }

    @Override
    public NewConfigFunction<ShortestPathDijkstraMutateConfig> newConfigFunction() {
        return (___,config) -> ShortestPathDijkstraMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Dijkstra, PathFindingResult, ShortestPathDijkstraMutateConfig, Stream<PathFindingMutateResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
