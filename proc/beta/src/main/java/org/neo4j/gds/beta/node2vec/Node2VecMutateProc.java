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
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecAlgorithmFactory;
import org.neo4j.gds.embeddings.node2vec.Node2VecModel;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardMutateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.node2vec.mutate", description = Node2VecCompanion.DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class Node2VecMutateProc extends MutatePropertyProc<Node2Vec, Node2VecModel.Result, Node2VecMutateProc.MutateResult, Node2VecMutateConfig> {

    @Procedure(value = "gds.beta.node2vec.mutate", mode = READ)
    @Description(Node2VecCompanion.DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecMutateConfig> computationResult = compute(
            graphName,
            configuration
        );

        return mutate(computationResult);
    }

    @Procedure(value = "gds.beta.node2vec.mutate.estimate", mode = READ)
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected Node2VecMutateConfig newConfig(String username, CypherMapWrapper config) {
        return Node2VecMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Node2Vec, Node2VecMutateConfig> algorithmFactory() {
        return new Node2VecAlgorithmFactory<>();
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecMutateConfig> computationResult) {
        return Node2VecCompanion.nodeProperties(computationResult);
    }

    @Override
    protected MutateResult.Builder resultBuilder(
        ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        var builder = new MutateResult.Builder();
        computeResult.result().ifPresent(result -> {
            builder.withLossPerIteration(result.lossPerIteration());
        });

        return builder;
    }

    public static final class MutateResult extends StandardMutateResult {

        public final long nodeCount;
        public final long nodePropertiesWritten;
        public final List<Double> lossPerIteration;

        MutateResult(
            long nodeCount,
            long nodePropertiesWritten,
            long preProcessingMillis,
            long computeMillis,
            long mutateMillis,
            Map<String, Object> configuration,
            List<Double> lossPerIteration
        ) {
            super(preProcessingMillis, computeMillis, 0, mutateMillis, configuration);
            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.lossPerIteration = lossPerIteration;
        }

        static class Builder extends AbstractResultBuilder<MutateResult> {

            private List<Double> lossPerIteration;

            @Override
            public MutateResult build() {
                return new MutateResult(
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
