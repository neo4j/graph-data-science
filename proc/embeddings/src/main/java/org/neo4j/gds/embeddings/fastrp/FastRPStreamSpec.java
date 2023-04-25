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
package org.neo4j.gds.embeddings.fastrp;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.fastRP.stream", description = FastRPCompanion.DESCRIPTION, executionMode = STREAM)
public class FastRPStreamSpec  implements AlgorithmSpec<FastRP, FastRP.FastRPResult, FastRPStreamConfig, Stream<StreamResult>, FastRPFactory<FastRPStreamConfig>> {
    @Override
    public String name() {
        return "FastRPStream";
    }

    @Override
    public FastRPFactory<FastRPStreamConfig> algorithmFactory() {
        return new FastRPFactory<>();
    }

    @Override
    public NewConfigFunction<FastRPStreamConfig> newConfigFunction() {
        return (__, userInput) -> FastRPStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<FastRP, FastRP.FastRPResult, FastRPStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> {

                if (computationResult.result().isEmpty()) {
                    return Stream.empty();
                }

                var graph = computationResult.graph();
                var nodePropertyValues = FastRPCompanion.nodeProperties(computationResult);
                return LongStream
                    .range(IdMap.START_NODE_ID, nodePropertyValues.nodeCount())
                    .filter(nodePropertyValues::hasValue)
                    .mapToObj(nodeId -> new StreamResult(graph.toOriginalNodeId(nodeId), nodePropertyValues.floatArrayValue(nodeId)));
            }
        );
    }
}
