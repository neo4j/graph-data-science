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
package org.neo4j.gds.beta.k1coloring;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.beta.k1coloring.K1ColoringSpecificationHelper.K1_COLORING_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.beta.k1coloring.stream", description = K1_COLORING_DESCRIPTION, executionMode = STREAM)
public class K1ColoringStreamSpecification implements AlgorithmSpec<K1Coloring, HugeLongArray, K1ColoringStreamConfig, Stream<K1ColoringStreamResult>, K1ColoringFactory<K1ColoringStreamConfig>> {

    @Override
    public String name() {
        return "K1ColoringStream";
    }

    @Override
    public K1ColoringFactory<K1ColoringStreamConfig> algorithmFactory() {
        return new K1ColoringFactory<>();
    }

    @Override
    public NewConfigFunction<K1ColoringStreamConfig> newConfigFunction() {
        return (__, userInput) -> K1ColoringStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<K1Coloring, HugeLongArray, K1ColoringStreamConfig, Stream<K1ColoringStreamResult>> computationResultConsumer() {
        return (ComputationResult<K1Coloring, HugeLongArray, K1ColoringStreamConfig> computationResult, ExecutionContext executionContext) ->
            runWithExceptionLogging("Result streaming failed", executionContext.log(), () -> {
                    if (computationResult.isGraphEmpty()) {
                        return Stream.empty();
                    }

                    var graph = computationResult.graph();
                    var config = computationResult.config();
                    var nodePropertyValues = (NodePropertyValues) CommunityProcCompanion.considerSizeFilter(
                        config,
                        computationResult.result()
                            .map(HugeLongArray::asNodeProperties)
                            .orElse(EmptyLongNodePropertyValues.INSTANCE)
                    );
                    return LongStream
                        .range(IdMap.START_NODE_ID, graph.nodeCount())
                        .filter(nodePropertyValues::hasValue)
                        .mapToObj(nodeId -> new K1ColoringStreamResult(
                            graph.toOriginalNodeId(nodeId),
                            nodePropertyValues.longValue(nodeId)
                        ));
                }
            );
    }
}
