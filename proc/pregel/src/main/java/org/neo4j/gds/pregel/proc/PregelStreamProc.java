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
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.beta.pregel.Element;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.executor.ComputationResult;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public abstract class PregelStreamProc<
    ALGO extends Algorithm<PregelResult>,
    CONFIG extends PregelProcedureConfig>
    extends StreamProc<ALGO, PregelResult, PregelStreamResult, CONFIG> {

    @Override
    protected Stream<PregelStreamResult> stream(
        ComputationResult<ALGO, PregelResult, CONFIG> computationResult
    ) {
        if (computationResult.result().isEmpty()) {
            return Stream.empty();
        }
        var result = computationResult.result().get().nodeValues();
        return LongStream.range(IdMap.START_NODE_ID, computationResult.graph().nodeCount()).mapToObj(nodeId -> {
            Map<String, Object> values = result.schema().elements()
                .stream()
                .filter(element -> element.visibility() == PregelSchema.Visibility.PUBLIC)
                .collect(Collectors.toMap(
                    Element::propertyKey,
                    element -> {
                        switch (element.propertyType()) {
                            case LONG:
                                return result.longProperties(element.propertyKey()).get(nodeId);
                            case DOUBLE:
                                return result.doubleProperties(element.propertyKey()).get(nodeId);
                            case DOUBLE_ARRAY:
                                return result.doubleArrayProperties(element.propertyKey()).get(nodeId);
                            case LONG_ARRAY:
                                return result.longArrayProperties(element.propertyKey()).get(nodeId);
                            default:
                                throw new IllegalArgumentException("Unsupported property type: " + element.propertyType());
                        }
                    }
                ));
            return new PregelStreamResult(computationResult.graph().toOriginalNodeId(nodeId), values);
        });

    }
}
