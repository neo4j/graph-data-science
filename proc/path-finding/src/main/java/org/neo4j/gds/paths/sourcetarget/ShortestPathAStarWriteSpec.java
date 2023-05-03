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
import org.neo4j.gds.paths.ShortestPathWriteResultConsumer;
import org.neo4j.gds.paths.astar.AStar;
import org.neo4j.gds.paths.astar.AStarFactory;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;
import static org.neo4j.gds.paths.sourcetarget.ShortestPathAStarCompanion.ASTAR_DESCRIPTION;

@GdsCallable(name = "gds.shortestPath.astar.write", description = ASTAR_DESCRIPTION, executionMode = WRITE_RELATIONSHIP)
public class ShortestPathAStarWriteSpec implements AlgorithmSpec<AStar, PathFindingResult, ShortestPathAStarWriteConfig, Stream<StandardWriteRelationshipsResult>, AStarFactory<ShortestPathAStarWriteConfig>> {

    @Override
    public String name() {
        return "AStarWrite";
    }

    @Override
    public AStarFactory<ShortestPathAStarWriteConfig> algorithmFactory() {
        return new AStarFactory<>();
    }

    @Override
    public NewConfigFunction<ShortestPathAStarWriteConfig> newConfigFunction() {
        return (___,config) -> ShortestPathAStarWriteConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<AStar, PathFindingResult, ShortestPathAStarWriteConfig, Stream<StandardWriteRelationshipsResult>> computationResultConsumer() {
        return new ShortestPathWriteResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
