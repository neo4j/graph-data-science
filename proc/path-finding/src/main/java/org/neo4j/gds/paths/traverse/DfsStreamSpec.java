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
package org.neo4j.gds.paths.traverse;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.PathFactory;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.paths.traverse.DfsStreamProc.DESCRIPTION;
import static org.neo4j.gds.paths.traverse.DfsStreamProc.NEXT;

@GdsCallable(name = "gds.dfs.stream", description = DESCRIPTION, executionMode = STREAM)
public class DfsStreamSpec implements AlgorithmSpec<DFS, long[], DfsStreamConfig, Stream<DfsStreamProc.DfsStreamResult>, DFSAlgorithmFactory> {
    @Override
    public String name() {
        return "DfsStream";
    }

    @Override
    public DFSAlgorithmFactory algorithmFactory() {
        return new DFSAlgorithmFactory();
    }

    @Override
    public NewConfigFunction<DfsStreamConfig> newConfigFunction() {
        return (__, config) -> DfsStreamConfig.of(config);
    }
    @Override
    public ComputationResultConsumer<DFS, long[], DfsStreamConfig, Stream<DfsStreamProc.DfsStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var graph = computationResult.graph();
            long[] nodes = computationResult.result();
            if (graph.isEmpty() || null == nodes) {
                return Stream.empty();
            }

            var nodeList = Arrays.stream(nodes).boxed().map(graph::toOriginalNodeId).collect(Collectors.toList());
            var startNode = computationResult.config().sourceNode();
            return Stream.of(new DfsStreamProc.DfsStreamResult(
                startNode,
                nodes,
                PathFactory.create(executionContext.transaction().internalTransaction(), nodeList, NEXT)
            ));
        };
    }
}
