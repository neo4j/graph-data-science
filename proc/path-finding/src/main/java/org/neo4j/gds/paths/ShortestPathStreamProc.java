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

import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.AlgoBaseConfig;

import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.toLowerCaseWithLocale;

public abstract class ShortestPathStreamProc<
    ALGO extends Algorithm<ALGO, DijkstraResult>,
    CONFIG extends AlgoBaseConfig> extends StreamProc<ALGO, DijkstraResult, StreamResult, CONFIG> {

    @Override
    protected StreamResult streamResult(long originalNodeId, long internalNodeId, NodeProperties nodeProperties) {
        throw new UnsupportedOperationException("Shortest path algorithm handles result building individually.");
    }

    @Override
    public Stream<StreamResult> stream(AlgoBaseProc.ComputationResult<ALGO, DijkstraResult, CONFIG> computationResult) {
        return runWithExceptionLogging("Result streaming failed", () -> {
            var graph = computationResult.graph();

            if (computationResult.isGraphEmpty()) {
                graph.release();
                return Stream.empty();
            }

            var shouldReturnPath = callContext
                .outputFields()
                .anyMatch(field -> toLowerCaseWithLocale(field).equals("path"));

            var resultBuilder = new StreamResult.Builder(graph, transaction.internalTransaction());

            var resultStream = computationResult
                .result()
                .mapPaths(path -> resultBuilder.build(path, shouldReturnPath));

            // this is necessary in order to close the result stream which triggers
            // the progress tracker to close its root task
            try(var statement = transaction.acquireStatement()) {
                statement.registerCloseableResource(resultStream);
            }
            return resultStream;
        });
    }
}
