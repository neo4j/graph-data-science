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
package org.neo4j.gds;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.Objects;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public abstract class StreamProc<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    PROC_RESULT,
    CONFIG extends AlgoBaseConfig> extends AlgoBaseProc<ALGO, ALGO_RESULT, CONFIG, PROC_RESULT> {

    protected abstract PROC_RESULT streamResult(long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues);

    @Override
    public ComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, Stream<PROC_RESULT>> computationResultConsumer() {
        return (ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult, ExecutionContext executionContext) ->
            runWithExceptionLogging("Result streaming failed", () -> {
                if (computationResult.isGraphEmpty()) {
                    return Stream.empty();
                }

                Graph graph = computationResult.graph();
                NodePropertyValues nodePropertyValues = nodeProperties(computationResult);
                return LongStream
                    .range(IdMap.START_NODE_ID, graph.nodeCount())
                    .filter(i -> nodePropertyValues.value(i) != null)
                    .mapToObj(nodeId -> streamResult(graph.toOriginalNodeId(nodeId), nodeId, nodePropertyValues));
                }
            );
    }

    protected Stream<PROC_RESULT> stream(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        return computationResultConsumer().consume(computationResult, executionContext());
    }
}
