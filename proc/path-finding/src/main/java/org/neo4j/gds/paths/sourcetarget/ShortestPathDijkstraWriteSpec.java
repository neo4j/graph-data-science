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
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraFactory;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;
import static org.neo4j.gds.paths.sourcetarget.ShortestPathDijkstraProc.DIJKSTRA_DESCRIPTION;

@GdsCallable(name = "gds.shortestPath.dijkstra.write", description = DIJKSTRA_DESCRIPTION, executionMode = WRITE_RELATIONSHIP)
public class ShortestPathDijkstraWriteSpec implements AlgorithmSpec<Dijkstra, DijkstraResult, ShortestPathDijkstraWriteConfig, Stream<StandardWriteRelationshipsResult>, DijkstraFactory.SourceTargetDijkstraFactory<ShortestPathDijkstraWriteConfig>> {

    @Override
    public String name() {
        return "DijkstraWrite";
    }

    @Override
    public DijkstraFactory.SourceTargetDijkstraFactory<ShortestPathDijkstraWriteConfig> algorithmFactory() {
        return new DijkstraFactory.SourceTargetDijkstraFactory<>();
    }

    @Override
    public NewConfigFunction<ShortestPathDijkstraWriteConfig> newConfigFunction() {
        return (___,config) -> ShortestPathDijkstraWriteConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Dijkstra, DijkstraResult, ShortestPathDijkstraWriteConfig, Stream<StandardWriteRelationshipsResult>> computationResultConsumer() {
        return new ShortestPathWriteResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
