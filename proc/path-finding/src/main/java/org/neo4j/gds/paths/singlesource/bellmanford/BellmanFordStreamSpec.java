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
package org.neo4j.gds.paths.singlesource.bellmanford;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
import org.neo4j.gds.paths.bellmanford.BellmanFordAlgorithmFactory;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.bellmanford.BellmanFordStreamConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.bellmanFord.stream", description = BellmanFord.DESCRIPTION, executionMode = STREAM)
public class BellmanFordStreamSpec implements AlgorithmSpec<BellmanFord, BellmanFordResult, BellmanFordStreamConfig, Stream<StreamResult>, BellmanFordAlgorithmFactory<BellmanFordStreamConfig>> {

    @Override
    public String name() {
        return "BellmanFordStream";
    }

    @Override
    public BellmanFordAlgorithmFactory<BellmanFordStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new BellmanFordAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<BellmanFordStreamConfig> newConfigFunction() {
        return (username, configuration) -> BellmanFordStreamConfig.of(configuration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ComputationResultConsumer<BellmanFord, BellmanFordResult, BellmanFordStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {

            var graph = computationResult.graph();

            if (computationResult.result().isEmpty()) {
                return Stream.empty();
            }

            var shouldReturnPath = executionContext
                .returnColumns()
                .contains("route");

            var result = computationResult.result().get();
            var containsNegativeCycle = result.containsNegativeCycle();

            var resultBuilder = new StreamResult.Builder(graph, executionContext.nodeLookup())
                .withIsCycle(containsNegativeCycle);

            PathFindingResult algorithmResult;
            if (containsNegativeCycle) {
                algorithmResult = result.negativeCycles();
            } else {
                algorithmResult = result.shortestPaths();
            }

            var resultStream = algorithmResult.mapPaths(path -> resultBuilder.build(path, shouldReturnPath));

            // this is necessary in order to close the result stream which triggers
            // the progress tracker to close its root task
            executionContext.closeableResourceRegistry().register(resultStream);
            return resultStream;

        };
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
