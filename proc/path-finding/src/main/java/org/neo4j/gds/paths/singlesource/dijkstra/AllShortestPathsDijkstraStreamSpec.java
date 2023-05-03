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

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.ShortestPathStreamResultConsumer;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraFactory;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(name = "gds.allShortestPaths.dijkstra.stream", description = Dijkstra.DESCRIPTION_SOURCE_TARGET, executionMode = MUTATE_RELATIONSHIP)
public class AllShortestPathsDijkstraStreamSpec implements AlgorithmSpec<Dijkstra, PathFindingResult, AllShortestPathsDijkstraStreamConfig, Stream<StreamResult>, DijkstraFactory.AllShortestPathsDijkstraFactory<AllShortestPathsDijkstraStreamConfig>> {

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
    public ComputationResultConsumer<Dijkstra, PathFindingResult, AllShortestPathsDijkstraStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return new ShortestPathStreamResultConsumer<>();
    }

@Override
    public boolean releaseProgressTask() {
        return false;
    }
}
