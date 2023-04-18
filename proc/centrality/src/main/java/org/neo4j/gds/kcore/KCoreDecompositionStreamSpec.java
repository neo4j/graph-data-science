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
package org.neo4j.gds.kcore;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.kcore.KCoreDecomposition.KCORE_DESCRIPTION;

@GdsCallable(name = "gds.kcore.stream", description = KCORE_DESCRIPTION, executionMode = STREAM)
public class KCoreDecompositionStreamSpec implements AlgorithmSpec<KCoreDecomposition, KCoreDecompositionResult, KCoreDecompositionStreamConfig, Stream<StreamResult>, KCoreDecompositionAlgorithmFactory<KCoreDecompositionStreamConfig>> {
    @Override
    public String name() {
        return "KCoreStream";
    }

    @Override
    public KCoreDecompositionAlgorithmFactory<KCoreDecompositionStreamConfig> algorithmFactory() {
        return new KCoreDecompositionAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KCoreDecompositionStreamConfig> newConfigFunction() {
        return (__, userInput) -> KCoreDecompositionStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<KCoreDecomposition, KCoreDecompositionResult, KCoreDecompositionStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            ()  ->computationResult.result()
                .map(result -> {
                    var coreValues=result.coreValues();
                    var graph = computationResult.graph();
                    return LongStream
                        .range(IdMap.START_NODE_ID, graph.nodeCount())
                        .mapToObj(nodeId ->
                            new StreamResult(
                                graph.toOriginalNodeId(nodeId),
                                coreValues.get(nodeId)
                            ));
                }).orElseGet(Stream::empty));
    }


}
