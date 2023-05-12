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
package org.neo4j.gds.paths.topologicalsort;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.topologicalsort.TopologicalSort;
import org.neo4j.gds.topologicalsort.TopologicalSortFactory;
import org.neo4j.gds.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.topologicalsort.TopologicalSortStreamConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.alpha.topologicalSort.stream", description = TopologicalSortStreamProc.TOPOLOGICAL_SORT_DESCRIPTION, executionMode = STREAM)
public class TopologicalSortStreamSpec implements AlgorithmSpec<TopologicalSort, TopologicalSortResult, TopologicalSortStreamConfig, Stream<TopologicalSortStreamResult>, TopologicalSortFactory<TopologicalSortStreamConfig>> {

    @Override
    public String name() {
        return "TopologicalSortStream";
    }

    @Override
    public TopologicalSortFactory<TopologicalSortStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new TopologicalSortFactory<>();
    }

    @Override
    public NewConfigFunction<TopologicalSortStreamConfig> newConfigFunction() {
        return (___, config) -> TopologicalSortStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<TopologicalSort, TopologicalSortResult, TopologicalSortStreamConfig, Stream<TopologicalSortStreamResult>> computationResultConsumer() {
        return (computationResult,  executionContext) ->
        {
            if (computationResult.result().isEmpty()) {
                return Stream.empty();
            }

            Graph graph = computationResult.graph();
            var topologicallySortedNodes = computationResult.result().get().value();
            return LongStream.range(0, graph.nodeCount())
                .mapToObj(nodeId -> new TopologicalSortStreamResult(graph.toOriginalNodeId(topologicallySortedNodes.get(nodeId))));
        };
    }
}
