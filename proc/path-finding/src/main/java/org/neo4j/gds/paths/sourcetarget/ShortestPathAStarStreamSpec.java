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

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.ShortestPathStreamResultConsumer;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.gds.paths.astar.AStar;
import org.neo4j.gds.paths.astar.AStarFactory;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarStreamConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.paths.sourcetarget.ShortestPathAStarCompanion.ASTAR_DESCRIPTION;

@GdsCallable(name = "gds.shortestPath.astar.stream", description = ASTAR_DESCRIPTION, executionMode = STREAM)
public class ShortestPathAStarStreamSpec implements AlgorithmSpec<AStar, DijkstraResult, ShortestPathAStarStreamConfig, Stream<StreamResult>, AStarFactory<ShortestPathAStarStreamConfig>> {

    @Override
    public String name() {
        return "AStarWrite";
    }

    @Override
    public AStarFactory<ShortestPathAStarStreamConfig> algorithmFactory() {
        return new AStarFactory<>();
    }

    @Override
    public NewConfigFunction<ShortestPathAStarStreamConfig> newConfigFunction() {
        return (___,config) -> ShortestPathAStarStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<AStar, DijkstraResult, ShortestPathAStarStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return new ShortestPathStreamResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
