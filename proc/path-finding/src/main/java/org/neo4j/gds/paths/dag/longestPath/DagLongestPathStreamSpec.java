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

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.dag.longestPath.DagLongestPath;
import org.neo4j.gds.dag.longestPath.DagLongestPathFactory;
import org.neo4j.gds.dag.longestPath.DagLongestPathStreamConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.function.LongToDoubleFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.dag.longestPath.stream", description = DagLongestPathStreamProc.LONGEST_PATH_DESCRIPTION, executionMode = STREAM)
public class DagLongestPathStreamSpec implements AlgorithmSpec<DagLongestPath, TopologicalSortResult, DagLongestPathStreamConfig, Stream<DagLongestPathStreamResult>, DagLongestPathFactory<DagLongestPathStreamConfig>> {

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
    public ComputationResultConsumer<DagLongestPath, TopologicalSortResult, DagLongestPathStreamConfig, Stream<DagLongestPathStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var graph = computationResult.graph();
                    var distances = result.maxSourceDistances().orElseGet(() -> {
                        executionContext.log().error("maxSourceDistances must be true in DAG Longest Path");
                        return null;
                    });
                    LongToDoubleFunction distanceFunction = distances != null
                    ? (nodeId) -> distances.get(nodeId)
                    : (nodeId) ->  0;
                    var topologicallySortedNodes = result.sortedNodes();

                    return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
                        .mapToObj(index -> {
                            var mappedNodeId = topologicallySortedNodes.get(index);
                            return new DagLongestPathStreamResult(
                                graph.toOriginalNodeId(mappedNodeId),
                                distanceFunction.applyAsDouble(mappedNodeId)
                            );
                        });
                }).orElseGet(Stream::empty)
        );
    }
}
