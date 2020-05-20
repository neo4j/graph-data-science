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
package org.neo4j.graphalgo.node2vec;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.node2vec.Node2Vec;
import org.neo4j.graphalgo.impl.node2vec.Node2VecConfig;
import org.neo4j.logging.Log;
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

public class Node2VecStreamProc extends StreamProc<Node2Vec, Node2Vec, Node2VecStreamProc.StreamResult, Node2VecConfig> {

    static final String NODE2VEC_DESCRIPTION = "The Node2Vec algorithm computes embeddings for nodes based on random walks.";

    @Procedure(value = "gds.alpha.node2vec.stream", mode = READ)
    @Description(NODE2VEC_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    )  {
        ComputationResult<Node2Vec, Node2Vec, Node2VecConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        if (computationResult.isGraphEmpty()) {
            return Stream.empty();
        }
        Node2Vec algorithm = computationResult.algorithm();
        Graph graph = computationResult.graph();
        return LongStream.range(0, graph.nodeCount()).mapToObj(nodeId -> new StreamResult(nodeId, algorithm.embeddingForNode(nodeId)));
    }

    @Override
    protected Node2VecConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return Node2VecConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Node2Vec, Node2VecConfig> algorithmFactory(Node2VecConfig config) {
        return new AlphaAlgorithmFactory<>() {
            @Override
            public Node2Vec buildAlphaAlgo(
                Graph graph, Node2VecConfig configuration, AllocationTracker tracker, Log log
            ) {
                return new Node2Vec(graph, config);
            }
        };
    }

    @Override
    protected StreamResult streamResult(long originalNodeId, double value) {
        throw new UnsupportedOperationException( "Node2VecStreamProc doesn't want to build results this way. He won't be just another brick in the wall, man.");
    }

    public static class StreamResult {
        public long nodeId;
        public List<Double> vector;

        StreamResult(long nodeId, double[] vector) {
            this.nodeId = nodeId;
            this.vector = Arrays.stream(vector).boxed().collect(Collectors.toList());
        }
    }
}
