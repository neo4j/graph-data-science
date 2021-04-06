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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.lang.Math.multiplyExact;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class Node2VecStreamProc extends StreamProc<Node2Vec, HugeObjectArray<Vector>, Node2VecStreamProc.StreamResult, Node2VecStreamConfig> {

    static final String NODE2VEC_DESCRIPTION = "The Node2Vec algorithm computes embeddings for nodes based on random walks.";

    @Procedure(value = "gds.alpha.node2vec.stream", mode = READ)
    @Description(NODE2VEC_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Node2Vec, HugeObjectArray<Vector>, Node2VecStreamConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        if (computationResult.isGraphEmpty()) {
            return Stream.empty();
        }
        Graph graph = computationResult.graph();
        var result = computationResult.result();

        return LongStream
            .range(0, graph.nodeCount())
            .mapToObj(nodeId -> new StreamResult(graph.toOriginalNodeId(nodeId), result.get(nodeId).data()));
    }

    @Procedure(value = "gds.alpha.node2vec.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected Node2VecStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return Node2VecStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Node2Vec, Node2VecStreamConfig> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            public Node2Vec build(
                Graph graph, Node2VecStreamConfig configuration, AllocationTracker tracker, Log log,
                ProgressEventTracker eventTracker
            ) {
                var progressLogger = new BatchingProgressLogger(
                    log,
                    0, //dummy value, gets overridden
                    "Node2Vec",
                    configuration.concurrency(),
                    eventTracker
                );
                validateConfig(configuration, graph);
                return new Node2Vec(graph, configuration, progressLogger, tracker);
            }

            @Override
            public MemoryEstimation memoryEstimation(Node2VecStreamConfig configuration) {
                return Node2Vec.memoryEstimation(configuration);
            }

            private void validateConfig(Node2VecStreamConfig config, Graph graph) {
                try {
                    var ignored = multiplyExact(multiplyExact(graph.nodeCount(), config.walksPerNode()), config.walkLength());
                } catch (ArithmeticException ex) {
                    throw new IllegalArgumentException(
                        formatWithLocale(
                            "Aborting execution, running with the configured parameters is likely to overflow: node count: %d, walks per node: %d, walkLength: %d." +
                            " Try reducing these parameters or run on a smaller graph.",
                            graph.nodeCount(),
                            config.walksPerNode(),
                            config.walkLength()
                        ));
                }
            }

        };
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        throw new UnsupportedOperationException(
            "Node2VecStreamProc doesn't want to build results this way. He won't be just another brick in the wall, man.");
    }

    @SuppressWarnings("unused")
    public static class StreamResult {
        public long nodeId;
        public List<Double> embedding;

        StreamResult(long nodeId, float[] embedding) {
            this.nodeId = nodeId;
            this.embedding = new ArrayList<>(embedding.length);
            for (var f : embedding) {
                this.embedding.add((double) f);
            }
        }
    }
}
