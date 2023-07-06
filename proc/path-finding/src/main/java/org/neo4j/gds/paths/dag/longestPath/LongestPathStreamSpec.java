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
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.topologicalsort.LongestPathStreamConfig;
import org.neo4j.gds.topologicalsort.TopologicalSort;
import org.neo4j.gds.topologicalsort.TopologicalSortFactory;
import org.neo4j.gds.topologicalsort.TopologicalSortResult;

import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.alpha.longestPath.stream", description = LongestPathStreamProc.LONGEST_PATH_DESCRIPTION, executionMode = STREAM)
public class LongestPathStreamSpec implements AlgorithmSpec<TopologicalSort, TopologicalSortResult, LongestPathStreamConfig, Stream<LongestPathStreamResult>, TopologicalSortFactory<LongestPathStreamConfig>> {

    @Override
    public String name() {
        return "dagLongestPathStream";
    }

    @Override
    public TopologicalSortFactory<LongestPathStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new TopologicalSortFactory<>();
    }

    @Override
    public NewConfigFunction<LongestPathStreamConfig> newConfigFunction() {
        return (___, config) -> LongestPathStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<TopologicalSort, TopologicalSortResult, LongestPathStreamConfig, Stream<LongestPathStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var graph = computationResult.graph();
                    var distances = result.longestPathDistances().orElse(null);
                    LongFunction<Double> distanceFunction = distances != null
                    ? (nodeId) -> distances.get(nodeId)
                    : (nodeId) ->  null;
                    var topologicallySortedNodes = result.sortedNodes();

                    return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
                        .mapToObj(index -> {
                            var mappedNodeId = topologicallySortedNodes.get(index);
                            return new LongestPathStreamResult(
                                graph.toOriginalNodeId(mappedNodeId),
                                distanceFunction.apply(mappedNodeId)
                            );
                        });
                }).orElseGet(Stream::empty)
        );
    }
}
