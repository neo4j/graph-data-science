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
package org.neo4j.gds.paths;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.procedures.pathfinding.PathFindingStreamResult;

import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

public final class ShortestPathStreamResultConsumer<ALGO extends Algorithm<PathFindingResult>, CONFIG extends AlgoBaseConfig>
    implements
    ComputationResultConsumer<ALGO, PathFindingResult, CONFIG, Stream<PathFindingStreamResult>> {

    @Override
    public Stream<PathFindingStreamResult> consume(
        ComputationResult<ALGO, PathFindingResult, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        return runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var graph = computationResult.graph();
                    var shouldReturnPath = executionContext.returnColumns()
                        .contains("path") && computationResult.graphStore().capabilities().canWriteToLocalDatabase();

                    var resultBuilder = new PathFindingStreamResult.Builder(graph, executionContext.nodeLookup());

                    var resultStream = result.mapPaths(path -> resultBuilder.build(path, shouldReturnPath));

                    // this is necessary in order to close the result stream which triggers
                    // the progress tracker to close its root task
                    executionContext.closeableResourceRegistry().register(resultStream);

                    return resultStream;
                })
                .orElseGet(Stream::empty)
        );
    }
}
