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

package org.neo4j.graphalgo.nodesim;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarity;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityResult;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityStreamConfig;
import org.neo4j.graphalgo.impl.nodesim.SimilarityResult;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class NodeSimilarityStreamProc extends NodeSimilarityProcBase<NodeSimilarityStreamConfig> {

    @Procedure(name = "gds.algo.nodeSimilarity.stream", mode = Mode.READ)
    @Description("CALL gds.algo.nodeSimilarity.stream(graphName: STRING, configuration: MAP {" +
                 "    similarityCutoff: 0.0," +
                 "    degreeCutoff: 0," +
                 "    topK: 10," +
                 "    bottomK: 10," +
                 "    topN: 0," +
                 "    bottomN: 0," +
                 "    concurrency: 4," +
                 "    readConcurrency: 4" +
                 "  }" +
                 ") YIELD" +
                 "  node1," +
                 "  node2," +
                 "  similarity")
    public Stream<SimilarityResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStreamConfig> result = compute(
            graphNameOrConfig,
            configuration
        );
        Graph graph = result.graph();

        if (result.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        return result.result().maybeStreamResult().get()
            .map(similarityResult -> {
                similarityResult.node1 = graph.toOriginalNodeId(similarityResult.node1);
                similarityResult.node2 = graph.toOriginalNodeId(similarityResult.node2);
                return similarityResult;
            });
    }

    @Override
    protected NodeSimilarityStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return NodeSimilarityStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }
}
