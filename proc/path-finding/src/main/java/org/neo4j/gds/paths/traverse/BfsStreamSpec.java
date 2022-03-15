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

import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.graphdb.Path;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.paths.traverse.BfsStreamProc.DESCRIPTION;
import static org.neo4j.gds.paths.traverse.BfsStreamProc.NEXT;
import static org.neo4j.gds.utils.StringFormatting.toLowerCaseWithLocale;

@GdsCallable(name = "gds.bfs.stream", description = DESCRIPTION, executionMode = STREAM)
public class BfsStreamSpec implements AlgorithmSpec<BFS, HugeLongArray, BfsStreamConfig, Stream<BfsStreamResult>, BfsAlgorithmFactory<BfsStreamConfig>> {
    @Override
    public String name() {
        return "BfsStream";
    }

    @Override
    public BfsAlgorithmFactory<BfsStreamConfig> algorithmFactory() {
        return new BfsAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<BfsStreamConfig> newConfigFunction() {
        return (__, config) -> BfsStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<BFS, HugeLongArray, BfsStreamConfig, Stream<BfsStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var graph = computationResult.graph();
            var result = computationResult.result();
            if (graph.isEmpty() || null == result) {
                return Stream.empty();
            }

            var nodes = result.toArray();
            var nodeList = Arrays.stream(nodes)
                .boxed()
                .map(graph::toOriginalNodeId)
                .collect(Collectors.toList());

            var shouldReturnPath = executionContext.callContext()
                .outputFields()
                .anyMatch(field -> toLowerCaseWithLocale(field).equals("path"));

            Path path = null;
            if(shouldReturnPath) {
                path = PathFactory.create(
                    executionContext.transaction().internalTransaction(),
                    nodeList,
                    NEXT
                );
            }

            var startNode = computationResult.config().sourceNode();
            return Stream.of(new BfsStreamResult(
                startNode,
                nodeList,
                path
            ));
        };
    }
}
