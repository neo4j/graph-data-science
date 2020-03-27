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
package org.neo4j.graphalgo.louvain;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.louvain.LouvainProc.LOUVAIN_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class LouvainStreamProc extends StreamProc<Louvain, Louvain, LouvainStreamProc.StreamResult, LouvainStreamConfig> {

    @Procedure(value = "gds.louvain.stream", mode = READ)
    @Description(LOUVAIN_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.louvain.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected LouvainStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LouvainStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Louvain, LouvainStreamConfig> algorithmFactory(LouvainStreamConfig config) {
        return new LouvainFactory<>();
    }

    @Override
    protected StreamResult streamResult(long nodeId, long originalNodeId, Louvain computationResult) {
        boolean includeIntermediateCommunities = computationResult.config().includeIntermediateCommunities();
        long[] communities = includeIntermediateCommunities ? computationResult.getCommunities(nodeId) : null;
        return new StreamResult(originalNodeId, communities, computationResult.getCommunity(nodeId));
    }

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
