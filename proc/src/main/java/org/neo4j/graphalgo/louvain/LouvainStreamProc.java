/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.ExecutionMode;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.LouvainStreamConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LouvainStreamProc extends LouvainProcBase<LouvainStreamConfig> {

    @Procedure(value = "gds.algo.louvain.stream", mode = READ)
    @Description("CALL gds.algo.louvain.stream(graphName: STRING, configuration: MAP {" +
                 "    maxIteration: INTEGER" +
                 "    maxLevels: INTEGER" +
                 "    tolerance: FLOAT" +
                 "    includeIntermediateCommunities: BOOLEAN" +
                 "    seedProperty: STRING" +
                 "  }" +
                 ") YIELD" +
                 "  nodeId: INTEGER" +
                 "  communityId: INTEGER" +
                 "  communityIds: LIST OF INTEGER")
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Louvain, Louvain, LouvainStreamConfig> computationResult = compute(
            graphNameOrConfig,
            configuration,
            ExecutionMode.STREAM
        );
        return stream(computationResult);
    }

    private Stream<StreamResult> stream(ComputationResult<Louvain, Louvain, LouvainStreamConfig> computationResult) {
        Graph graph = computationResult.graph();
        Louvain louvain = computationResult.result();
        boolean includeIntermediateCommunities = computationResult.config().includeIntermediateCommunities();
        return LongStream.range(0, graph.nodeCount())
            .mapToObj(nodeId -> {
                long neoNodeId = graph.toOriginalNodeId(nodeId);
                long[] communities = includeIntermediateCommunities ? louvain.getCommunities(nodeId) : null;
                return new StreamResult(neoNodeId, communities, louvain.getCommunity(nodeId));
            });
    }

    @Override
    LouvainStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LouvainStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    public static final class StreamResult {
        public final long nodeId;
        public final long communityId;
        public final List<Long> communityIds;

        StreamResult(long nodeId, @Nullable long[] communityIds, long communityId) {
            this.nodeId = nodeId;
            this.communityIds = communityIds == null ? null : Arrays
                .stream(communityIds)
                .boxed()
                .collect(Collectors.toList());
            this.communityId = communityId;
        }
    }
}
