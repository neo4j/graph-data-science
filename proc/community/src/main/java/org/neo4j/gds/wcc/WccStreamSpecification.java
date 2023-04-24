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
package org.neo4j.gds.wcc;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.wcc.WccProc.WCC_DESCRIPTION;

@GdsCallable(name = "gds.wcc.stream", description = WCC_DESCRIPTION, executionMode = ExecutionMode.STREAM)
public class WccStreamSpecification implements AlgorithmSpec<Wcc, DisjointSetStruct, WccStreamConfig, Stream<WccStreamProc.StreamResult>, WccAlgorithmFactory<WccStreamConfig>> {

    @Override
    public String name() {
        return "WccStream";
    }

    @Override
    public WccAlgorithmFactory<WccStreamConfig> algorithmFactory() {
        return new WccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<WccStreamConfig> newConfigFunction() {
        return (__, userInput) -> WccStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Wcc, DisjointSetStruct, WccStreamConfig, Stream<WccStreamProc.StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.isGraphEmpty()) {
                return Stream.empty();
            }

            var graph = computationResult.graph();
            var nodePropertyValues = CommunityProcCompanion.nodeProperties(
                computationResult.config(),
                computationResult.result()
                    .map(DisjointSetStruct::asNodeProperties)
                    .orElse(EmptyLongNodePropertyValues.INSTANCE)
            );
            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .filter(nodePropertyValues::hasValue)
                .mapToObj(nodeId -> new WccStreamProc.StreamResult(
                    graph.toOriginalNodeId(nodeId),
                    nodePropertyValues.longValue(nodeId)
                ));
        };
    }
}
