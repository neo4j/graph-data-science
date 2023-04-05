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
package org.neo4j.gds.degree;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.common.CentralityStreamResult;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.degree.DegreeCentrality.DEGREE_CENTRALITY_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.degree.stream", description = DEGREE_CENTRALITY_DESCRIPTION, executionMode = STREAM)
public class DegreeCentralityStreamSpecification implements AlgorithmSpec<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStreamConfig, Stream<CentralityStreamResult>, DegreeCentralityFactory<DegreeCentralityStreamConfig>> {
    @Override
    public String name() {
        return "DegreeCentralityStream";
    }

    @Override
    public DegreeCentralityFactory<DegreeCentralityStreamConfig> algorithmFactory() {
        return new DegreeCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<DegreeCentralityStreamConfig> newConfigFunction() {
        return (__, userInput) -> DegreeCentralityStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStreamConfig, Stream<CentralityStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            ()  -> Optional.ofNullable(computationResult.result())
                .map(result -> {
                    var nodePropertyValues = DegreeCentralityNodePropertyValues.from(computationResult);
                    var graph = computationResult.graph();
                    return LongStream
                        .range(IdMap.START_NODE_ID, graph.nodeCount())
                        .filter(nodePropertyValues::hasValue)
                        .mapToObj(nodeId ->
                            new CentralityStreamResult(
                                graph.toOriginalNodeId(nodeId),
                                nodePropertyValues.doubleValue(nodeId)
                            ));
                }).orElseGet(Stream::empty)
        );    }
}
