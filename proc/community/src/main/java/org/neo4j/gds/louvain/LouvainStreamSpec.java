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
package org.neo4j.gds.louvain;

import org.neo4j.gds.CommunityProcCompanion;
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

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.louvain.LouvainConstants.DESCRIPTION;

@GdsCallable(name = "gds.louvain.stream", description = DESCRIPTION, executionMode = STREAM)
public class LouvainStreamSpec implements AlgorithmSpec<Louvain, LouvainResult, LouvainStreamConfig, Stream<StreamResult>, LouvainAlgorithmFactory<LouvainStreamConfig>> {
    @Override
    public String name() {
        return "LouvainStream";
    }

    @Override
    public LouvainAlgorithmFactory<LouvainStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new LouvainAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LouvainStreamConfig> newConfigFunction() {
        return (__, config) -> LouvainStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Louvain, LouvainResult, LouvainStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.result().isEmpty()) {
                return Stream.empty();
            }

            var graph = computationResult.graph();
            var nodeCount = graph.nodeCount();
            var nodePropertyValues = nodeProperties(computationResult);
            var includeIntermediateCommunities = computationResult.config().includeIntermediateCommunities();
            var louvainResult = computationResult.result().get();

            return LongStream.range(0, nodeCount)
                .boxed().
                filter(nodePropertyValues::hasValue)
                .map(nodeId -> {
                    long[] communities = includeIntermediateCommunities ? louvainResult.getIntermediateCommunities(
                        nodeId) : null;
                    long communityId = nodePropertyValues.longValue(nodeId);
                    return new StreamResult(graph.toOriginalNodeId(nodeId), communities, communityId);
                });
        };
    }

    protected NodePropertyValues nodeProperties(ComputationResult<Louvain, LouvainResult, LouvainStreamConfig> computationResult) {
        return getCommunities(computationResult);
    }

    private static <CONFIG extends LouvainBaseConfig> NodePropertyValues getCommunities(
        ComputationResult<Louvain, LouvainResult, CONFIG> computationResult
    ) {
        return CommunityProcCompanion.nodeProperties(
            computationResult.config(),
            computationResult.result()
                .map(LouvainResult::dendrogramManager)
                .map(LouvainDendrogramManager::getCurrent)
                .map(HugeLongArray::asNodeProperties)
                .orElse(EmptyLongNodePropertyValues.INSTANCE)
        );
    }
}
