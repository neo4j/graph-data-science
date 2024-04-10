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
package org.neo4j.gds.paths.dag.longestPath;

import org.neo4j.gds.dag.longestPath.DagLongestPath;
import org.neo4j.gds.dag.longestPath.DagLongestPathFactory;
import org.neo4j.gds.dag.longestPath.DagLongestPathStreamConfig;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.paths.ShortestPathStreamResultConsumer;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingStreamResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.dag.longestPath.stream", description = Constants.LONGEST_PATH_DESCRIPTION, executionMode = STREAM)
public class DagLongestPathStreamSpec implements AlgorithmSpec<DagLongestPath, PathFindingResult, DagLongestPathStreamConfig, Stream<PathFindingStreamResult>, DagLongestPathFactory<DagLongestPathStreamConfig>> {

    @Override
    public String name() {
        return "dagLongestPathStream";
    }

    @Override
    public DagLongestPathFactory<DagLongestPathStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new DagLongestPathFactory<>();
    }

    @Override
    public NewConfigFunction<DagLongestPathStreamConfig> newConfigFunction() {
        return (___, config) -> DagLongestPathStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<DagLongestPath, PathFindingResult, DagLongestPathStreamConfig, Stream<PathFindingStreamResult>> computationResultConsumer() {
        return new ShortestPathStreamResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
