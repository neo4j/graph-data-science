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
package org.neo4j.gds.paths.singlesource.dijkstra;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraFactory;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingStreamResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;
import static org.neo4j.gds.paths.singlesource.SingleSourceShortestPathConstants.DIJKSTRA_DESCRIPTION;

@GdsCallable(name = "gds.allShortestPaths.dijkstra.stream", description = DIJKSTRA_DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class AllShortestPathsDijkstraStreamSpec implements AlgorithmSpec<Dijkstra, PathFindingResult, AllShortestPathsDijkstraStreamConfig, Stream<PathFindingStreamResult>, DijkstraFactory.AllShortestPathsDijkstraFactory<AllShortestPathsDijkstraStreamConfig>> {

    @Override
    public String name() {
        return "gds.allShortestPaths.dijkstra.stream";
    }

    @Override
    public DijkstraFactory.AllShortestPathsDijkstraFactory<AllShortestPathsDijkstraStreamConfig> algorithmFactory(
        ExecutionContext executionContext
    ) {
        return new DijkstraFactory.AllShortestPathsDijkstraFactory<>();
    }

    @Override
    public NewConfigFunction<AllShortestPathsDijkstraStreamConfig> newConfigFunction() {
        return (username, configuration) -> AllShortestPathsDijkstraStreamConfig.of(configuration);
    }

    @Override
    public ComputationResultConsumer<Dijkstra, PathFindingResult, AllShortestPathsDijkstraStreamConfig, Stream<PathFindingStreamResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
