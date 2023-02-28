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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.louvain.stream", description = LouvainProc.LOUVAIN_DESCRIPTION, executionMode = STREAM)
public class LouvainStreamProc extends StreamProc<Louvain, LouvainResult, LouvainStreamProc.StreamResult, LouvainStreamConfig> {

    @Procedure(value = "gds.louvain.stream", mode = READ)
    @Description(LouvainProc.LOUVAIN_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphName, configuration));
    }

    @Procedure(value = "gds.louvain.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected LouvainStreamConfig newConfig(String username, CypherMapWrapper config) {
        return LouvainStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Louvain, LouvainStreamConfig> algorithmFactory() {
        return new LouvainFactory<>();
    }

    @Override
    protected Stream<StreamResult> stream(ComputationResult<Louvain, LouvainResult, LouvainStreamConfig> computationResult) {
        return runWithExceptionLogging("Graph streaming failed", () -> {
            Graph graph = computationResult.graph();

            boolean consecutiveIds = computationResult
                    .config()
                    .consecutiveIds();

            if (computationResult.isGraphEmpty()) {
                return Stream.empty();
            }

            var louvain = computationResult.result();
            var nodeProperties = nodeProperties(computationResult);


            return LongStream
                    .range(0, graph.nodeCount())
                    .boxed()
                    .filter(nodeProperties::hasValue)
                    .map(nodeId -> {
                        boolean includeIntermediateCommunities = computationResult
                                .config()
                                .includeIntermediateCommunities();

                        long[] communities = includeIntermediateCommunities ? louvain.getIntermediateCommunities(nodeId) : null;
                        long communityId = consecutiveIds
                            ? nodeProperties.longValue(nodeId)
                            : louvain.getCommunity(nodeId);

                        return new StreamResult(
                                graph.toOriginalNodeId(nodeId),
                                communities,
                                communityId
                        );
                    });
        });
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<Louvain, LouvainResult, LouvainStreamConfig> computationResult) {
        var config = computationResult.config();
        var includeIntermediateCommunities = config.includeIntermediateCommunities();

        var result = computationResult.result();

        if (!includeIntermediateCommunities) {
            return CommunityProcCompanion.nodeProperties(
                    computationResult.config(),
                    result.dendrogramManager().getCurrent().asNodeProperties()
            );
        }

        return LouvainProc.longArrayNodePropertyValues(computationResult, result);
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues
    ) {
        throw new UnsupportedOperationException("Louvain handles result building individually.");
    }

    @SuppressWarnings("unused")
    public static final class StreamResult {
        public final long nodeId;
        public final long communityId;
        public final List<Long> intermediateCommunityIds;

        StreamResult(long nodeId, @Nullable long[] intermediateCommunityIds, long communityId) {
            this.nodeId = nodeId;
            this.intermediateCommunityIds = intermediateCommunityIds == null ? null : Arrays
                .stream(intermediateCommunityIds)
                .boxed()
                .collect(Collectors.toList());
            this.communityId = communityId;
        }
    }
}
