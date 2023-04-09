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
package org.neo4j.gds.beta.node2vec;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecAlgorithmFactory;
import org.neo4j.gds.embeddings.node2vec.Node2VecModel;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.beta.node2vec.write", description = Node2VecCompanion.DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class Node2VecWriteProc extends WriteProc<Node2Vec, Node2VecModel.Result, Node2VecWriteProc.WriteResult, Node2VecWriteConfig> {

    @Procedure(value = "gds.beta.node2vec.write", mode = WRITE)
    @Description(Node2VecCompanion.DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecWriteConfig> computationResult = compute(
            graphName,
            configuration
        );
        return write(computationResult);
    }

    @Procedure(value = "gds.beta.node2vec.write.estimate", mode = READ)
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected Node2VecWriteConfig newConfig(String username, CypherMapWrapper config) {
        return Node2VecWriteConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Node2Vec, Node2VecWriteConfig> algorithmFactory() {
        return new Node2VecAlgorithmFactory<>();
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecWriteConfig> computationResult) {
        return Node2VecCompanion.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        WriteResult.Builder builder = new WriteResult.Builder();

        computeResult.result()
            .map(Node2VecModel.Result::lossPerIteration)
            .ifPresent(builder::withLossPerIteration);

        return builder;
    }

    @SuppressWarnings("unused")
    public static final class WriteResult {

        public final long nodeCount;
        public final long nodePropertiesWritten;
        public final long preProcessingMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final Map<String, Object> configuration;
        public final List<Double> lossPerIteration;

        WriteResult(
            long nodeCount,
            long nodePropertiesWritten,
            long preProcessingMillis,
            long computeMillis,
            long writeMillis,
            Map<String, Object> configuration,
            List<Double> lossPerIteration
        ) {
            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.preProcessingMillis = preProcessingMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.configuration = configuration;
            this.lossPerIteration = lossPerIteration;
        }

        static class Builder extends AbstractResultBuilder<WriteResult> {

            private List<Double> lossPerIteration;

            @Override
            public WriteResult build() {
                return new WriteResult(
                    nodeCount,
                    nodePropertiesWritten,
                    preProcessingMillis,
                    computeMillis,
                    writeMillis,
                    config.toMap(),
                    lossPerIteration
                );
            }

            Builder withLossPerIteration(List<Double> lossPerIteration) {
                this.lossPerIteration = lossPerIteration;
                return this;
            }
        }
    }
}
