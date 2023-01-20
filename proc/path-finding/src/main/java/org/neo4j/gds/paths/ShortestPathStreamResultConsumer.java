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
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.toLowerCaseWithLocale;

public final class ShortestPathStreamResultConsumer<ALGO extends Algorithm<DijkstraResult>, CONFIG extends AlgoBaseConfig> implements ComputationResultConsumer<ALGO, DijkstraResult, CONFIG, Stream<StreamResult>> {

    @Override
    public Stream<StreamResult> consume(
        ComputationResult<ALGO, DijkstraResult, CONFIG> computationResult, ExecutionContext executionContext
    ) {
        var graph = computationResult.graph();

        if (computationResult.isGraphEmpty()) {
            return Stream.empty();
        }

        var shouldReturnPath = executionContext.callContext()
            .outputFields()
            .anyMatch(field -> toLowerCaseWithLocale(field).equals("path"));

        var resultBuilder = new StreamResult.Builder(graph, executionContext.transaction().internalTransaction());

        var resultStream = computationResult
            .result()
            .mapPaths(path -> resultBuilder.build(path, shouldReturnPath));

        // this is necessary in order to close the result stream which triggers
        // the progress tracker to close its root task
        try (var statement = executionContext.transaction().acquireStatement()) {
            statement.registerCloseableResource(resultStream);
        }

        return resultStream;
    }
}
