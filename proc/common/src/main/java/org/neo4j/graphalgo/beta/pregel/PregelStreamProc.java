/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public abstract class PregelStreamProc<
    ALGO extends Algorithm<ALGO, Pregel.PregelResult>,
    CONFIG extends PregelConfig>
    extends StreamProc<ALGO, Pregel.PregelResult, PregelStreamResult, CONFIG> {

    @Override
    protected PregelStreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        throw new UnsupportedOperationException("This should never be called.");
    }

    @Override
    protected Stream<PregelStreamResult> stream(ComputationResult<ALGO, Pregel.PregelResult, CONFIG> computationResult) {
        if (computationResult.isGraphEmpty()) {
            return Stream.empty();
        }

        Graph graph = computationResult.graph();

        var result = computationResult.result().compositeNodeValues();
        var schema = result.schema();

        return LongStream
            .range(IdMapping.START_NODE_ID, graph.nodeCount())
            .mapToObj(nodeId -> {
                Map<String, Object> values = schema.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                        if (entry.getValue() == ValueType.DOUBLE) {
                            return result.doubleProperties(entry.getKey()).get(nodeId);
                        }
                        return result.longProperties(entry.getKey()).get(nodeId);
                    }
                ));
                return new PregelStreamResult(nodeId, values);
            });
    }
}
