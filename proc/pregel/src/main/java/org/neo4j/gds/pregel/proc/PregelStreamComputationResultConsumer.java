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
package org.neo4j.gds.pregel.proc;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.beta.pregel.Element;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

public class PregelStreamComputationResultConsumer<
    ALGO extends Algorithm<PregelResult>,
    CONFIG extends PregelProcedureConfig
  > implements ComputationResultConsumer<ALGO, PregelResult, CONFIG, Stream<PregelStreamResult>> {

    @Override
    public Stream<PregelStreamResult> consume(
        ComputationResult<ALGO, PregelResult, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        return runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> {
                var result = computationResult.result();
                if (result.isEmpty()) {
                    return Stream.empty();
                }
                var nodeValues = result.get().nodeValues();
                // TODO: reduce the inner iteration to what's visible upfront
                // TODO: map the element to a property lookup function upfront
                // for every node
                return LongStream.range(IdMap.START_NODE_ID, computationResult.graph().nodeCount()).mapToObj(nodeId -> {
                    // for every schema element
                    Map<String, Object> values = nodeValues.schema().elements()
                        .stream()
                        // if its visible
                        .filter(element -> element.visibility() == PregelSchema.Visibility.PUBLIC)
                        // collect a String->Object map
                        .collect(Collectors.toMap(
                            // of the element's property key
                            Element::propertyKey,
                            // to a value
                            element -> {
                                // retrieved based on the elements property type
                                switch (element.propertyType()) {
                                    case LONG:
                                        return nodeValues.longProperties(element.propertyKey()).get(nodeId);
                                    case DOUBLE:
                                        return nodeValues.doubleProperties(element.propertyKey()).get(nodeId);
                                    case DOUBLE_ARRAY:
                                        return nodeValues.doubleArrayProperties(element.propertyKey()).get(nodeId);
                                    case LONG_ARRAY:
                                        return nodeValues.longArrayProperties(element.propertyKey()).get(nodeId);
                                    default:
                                        throw new IllegalArgumentException("Unsupported property type: " + element.propertyType());
                                }
                            }
                        ));
                    return new PregelStreamResult(computationResult.graph().toOriginalNodeId(nodeId), values);
                });

            }
        );
    }
}
