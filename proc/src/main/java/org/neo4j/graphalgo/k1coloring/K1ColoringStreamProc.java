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

package org.neo4j.graphalgo.k1coloring;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.coloring.K1Coloring;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class K1ColoringStreamProc extends K1ColoringBaseProc<K1ColoringConfig> {

    @Procedure(name = "algo.beta.k1coloring.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<K1Coloring, K1Coloring, K1ColoringConfig> compute = compute(graphNameOrConfig, configuration);
        K1Coloring coloring = compute.result();

        Stream<StreamResult> stream;
        if (coloring == null) {
            stream = Stream.empty();
        } else {
            Graph graph = compute.graph();
            stream = LongStream.range(0, graph.nodeCount())
                .mapToObj(nodeId -> {
                    long neoNodeId = graph.toOriginalNodeId(nodeId);
                    return new StreamResult(neoNodeId, coloring.colors().get(nodeId));
                });
        }

        return stream;
    }

    @Override
    protected K1ColoringConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return K1ColoringConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    public static class StreamResult {
        public final long nodeId;
        public final long color;

        StreamResult(long nodeId, long color) {
            this.nodeId = nodeId;
            this.color = color;
        }
    }
}
