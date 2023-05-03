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
package org.neo4j.gds.leiden;

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
import static org.neo4j.gds.leiden.LeidenStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.beta.leiden.stream", description = DESCRIPTION, executionMode = STREAM)
public class LeidenStreamSpec implements AlgorithmSpec<Leiden, LeidenResult, LeidenStreamConfig, Stream<StreamResult>, LeidenAlgorithmFactory<LeidenStreamConfig>> {
    @Override
    public String name() {
        return "LeidenStream";
    }

    @Override
    public LeidenAlgorithmFactory<LeidenStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new LeidenAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LeidenStreamConfig> newConfigFunction() {
        return (__, config) -> LeidenStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Leiden, LeidenResult, LeidenStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.result().isEmpty()) {
                return Stream.empty();
            }

            var leidenResult = computationResult.result().get();
            var graph = computationResult.graph();
            var nodeProperties = nodeProperties(computationResult);

            boolean includeIntermediateCommunities = computationResult.config().includeIntermediateCommunities();

            return LongStream.range(0, graph.nodeCount())
                .filter(nodeProperties::hasValue)
                .mapToObj(nodeId -> {
                    long[] intermediateCommunityIds = includeIntermediateCommunities
                        ? leidenResult.getIntermediateCommunities(nodeId)
                        : null;
                    long communityId = nodeProperties.longValue(nodeId);

                    return new StreamResult(
                        graph.toOriginalNodeId(nodeId),
                        intermediateCommunityIds,
                        communityId
                    );
                });
        };
    }

    protected NodePropertyValues nodeProperties(ComputationResult<Leiden, LeidenResult, LeidenStreamConfig> computationResult) {
        return getCommunities(computationResult);
    }

    private static <CONFIG extends LeidenBaseConfig> NodePropertyValues getCommunities(
            ComputationResult<Leiden, LeidenResult, CONFIG> computationResult
    ) {
        return CommunityProcCompanion.nodeProperties(
            computationResult.config(),
            computationResult.result()
                .map(LeidenResult::dendrogramManager)
                .map(LeidenDendrogramManager::getCurrent)
                .map(HugeLongArray::asNodeProperties)
                .orElse(EmptyLongNodePropertyValues.INSTANCE)
        );
    }
}
