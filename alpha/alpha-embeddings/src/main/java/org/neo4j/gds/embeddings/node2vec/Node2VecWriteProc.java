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
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleArrayNodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ArrayUtil;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.Math.multiplyExact;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class Node2VecWriteProc extends WriteProc<Node2Vec, HugeObjectArray<Vector>, Node2VecWriteProc.WriteResult, Node2VecWriteConfig> {

    @Procedure(value = "gds.alpha.node2vec.write", mode = WRITE)
    @Description(Node2VecStreamProc.NODE2VEC_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Node2Vec, HugeObjectArray<Vector>, Node2VecWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult);
    }

    @Procedure(value = "gds.alpha.node2vec.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected Node2VecWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return Node2VecWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Node2Vec, Node2VecWriteConfig> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            public Node2Vec build(
                Graph graph, Node2VecWriteConfig configuration, AllocationTracker tracker, Log log,
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
            public MemoryEstimation memoryEstimation(Node2VecWriteConfig configuration) {
                return Node2Vec.memoryEstimation(configuration);
            }

            private void validateConfig(Node2VecWriteConfig config, Graph graph) {
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
    protected NodeProperties nodeProperties(ComputationResult<Node2Vec, HugeObjectArray<Vector>, Node2VecWriteConfig> computationResult) {
        return (DoubleArrayNodeProperties) (nodeId) -> ArrayUtil.floatToDoubleArray(computationResult.result().get(nodeId).data());
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(ComputationResult<Node2Vec, HugeObjectArray<Vector>, Node2VecWriteConfig> computeResult) {
        return new WriteResult.Builder();
    }

    @SuppressWarnings("unused")
    public static final class WriteResult {

        public final long nodeCount;
        public final long nodePropertiesWritten;
        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final Map<String, Object> configuration;

        WriteResult(
            long nodeCount,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            Map<String, Object> configuration
        ) {
            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.configuration = configuration;
        }

        static class Builder extends AbstractResultBuilder<WriteResult> {

            @Override
            public WriteResult build() {
                return new WriteResult(
                    nodeCount,
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    writeMillis,
                    config.toMap()
                );
            }
        }
    }

}
