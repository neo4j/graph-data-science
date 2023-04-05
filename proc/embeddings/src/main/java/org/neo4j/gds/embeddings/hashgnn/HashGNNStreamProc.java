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
package org.neo4j.gds.embeddings.hashgnn;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
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

import static org.neo4j.gds.embeddings.hashgnn.HashGNNProcCompanion.DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.hashgnn.stream", description = DESCRIPTION, executionMode = STREAM)
public class HashGNNStreamProc extends StreamProc<HashGNN, HashGNN.HashGNNResult, HashGNNStreamProc.StreamResult, HashGNNStreamConfig> {

    @Procedure(value = "gds.beta.hashgnn.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<HashGNN, HashGNN.HashGNNResult, HashGNNStreamConfig> computationResult = compute(
            graphName,
            configuration
        );
        return stream(computationResult);
    }

    @Procedure(value = "gds.beta.hashgnn.stream.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected Stream<HashGNNStreamProc.StreamResult> stream(ComputationResult<HashGNN, HashGNN.HashGNNResult, HashGNNStreamConfig> computationResult) {
        return runWithExceptionLogging("HashGNN streaming failed", () -> {
            var graph = computationResult.graph();
            var result = computationResult.result();

            if (result == null) {
                return Stream.empty();
            }

            return LongStream.range(0, graph.nodeCount())
                .mapToObj(i -> new HashGNNStreamProc.StreamResult(
                    graph.toOriginalNodeId(i),
                    result.embeddings().doubleArrayValue(i)
                ));
        });
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<HashGNN, HashGNN.HashGNNResult, HashGNNStreamConfig> computationResult) {
        return computationResult.result().embeddings();
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues
    ) {
        throw new UnsupportedOperationException("HashGNN handles result building individually.");
    }

    @Override
    protected HashGNNStreamConfig newConfig(String username, CypherMapWrapper config) {
        return HashGNNStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<HashGNN, HashGNNStreamConfig> algorithmFactory() {
        return new HashGNNFactory<>();
    }

    @SuppressWarnings("unused")
    public static final class StreamResult {
        public final long nodeId;
        public final List<Double> embedding;

        StreamResult(long nodeId, double[] embeddings) {
            this.nodeId = nodeId;
            this.embedding = Arrays.stream(embeddings).boxed().collect(Collectors.toList());
        }
    }
}
