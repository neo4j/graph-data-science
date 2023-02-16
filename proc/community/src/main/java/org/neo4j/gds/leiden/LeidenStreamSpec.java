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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.values.storable.LongValue;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
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
    public LeidenAlgorithmFactory<LeidenStreamConfig> algorithmFactory() {
        return new LeidenAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LeidenStreamConfig> newConfigFunction() {
        return (__, config) -> LeidenStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Leiden, LeidenResult, LeidenStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var leidenResult = computationResult.result();
            if (leidenResult == null) {
                return Stream.empty();
            }
            var graph = computationResult.graph();
            boolean includeIntermediateCommunities = computationResult.config().includeIntermediateCommunities();


            return LongStream.range(0, graph.nodeCount())
                .mapToObj(nodeId -> {
                    Optional<Long> communityId = getCommunityId(computationResult, nodeId);

                    return communityId.map(id -> new StreamResult(
                            graph.toOriginalNodeId(nodeId),
                            includeIntermediateCommunities ? leidenResult.getIntermediateCommunities(nodeId) : null,
                            id
                    )).orElse(null);
                })
                .filter(Objects::nonNull);
        };
    }

    @Nullable
    private Optional<Long> getCommunityId(ComputationResult<Leiden, LeidenResult, LeidenStreamConfig> computationResult, long nodeId) {
        var leidenResult = computationResult.result();
        var communities = leidenResult.communities();
        boolean consecutiveIds = computationResult.config().consecutiveIds();

        if (!consecutiveIds) {
            return Optional.of(communities.get(nodeId));
        }

        var nodeValue = nodeProperties(computationResult).value(nodeId);

        return Optional.ofNullable(nodeValue).map(value -> ((LongValue) value).value());

    }

    protected NodePropertyValues nodeProperties(ComputationResult<Leiden, LeidenResult, LeidenStreamConfig> computationResult) {
        return LeidenCompanion.leidenNodeProperties(computationResult, UUID.randomUUID().toString());
    }
}
