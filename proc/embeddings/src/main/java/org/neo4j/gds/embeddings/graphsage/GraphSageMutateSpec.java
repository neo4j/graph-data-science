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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.EmptyDoubleArrayNodePropertyValues;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.executor.validation.ValidationConfiguration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.GRAPHSAGE_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;

@GdsCallable(name = "gds.beta.graphSage.mutate", description = GRAPHSAGE_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class GraphSageMutateSpec implements AlgorithmSpec<GraphSage, GraphSageResult, GraphSageMutateConfig, Stream<MutateResult>, GraphSageAlgorithmFactory<GraphSageMutateConfig>> {

    private ModelCatalog modelCatalog;

    @Override
    public String name() {
        return "GraphSageMutate";
    }

    @Override
    public GraphSageAlgorithmFactory<GraphSageMutateConfig> algorithmFactory() {
        return new GraphSageAlgorithmFactory<>(modelCatalog);
    }

    @Override
    public NewConfigFunction<GraphSageMutateConfig> newConfigFunction() {
        return GraphSageMutateConfig::of;
    }

    @Override
    public ComputationResultConsumer<GraphSage, GraphSageResult, GraphSageMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            this::nodePropertyList,
            this::resultBuilder
        );
    }

    @Override
    public GraphSageMutateSpec withModelCatalog(ModelCatalog modelCatalog) {
        this.modelCatalog = modelCatalog;
        return this;
    }

    @Override
    public ValidationConfiguration<GraphSageMutateConfig> validationConfig(ExecutionContext executionContext) {
        return new GraphSageConfigurationValidation<>(modelCatalog);
    }

    private MutateResult.Builder resultBuilder(
        ComputationResult<GraphSage, GraphSageResult, GraphSageMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        return new MutateResult.Builder();
    }

    private List<NodeProperty> nodePropertyList(ComputationResult<GraphSage, GraphSageResult, GraphSageMutateConfig> computationResult) {
        return List.of(NodeProperty.of(
            computationResult.config().mutateProperty(),
            nodePropertyValues(computationResult.result())
        ));
    }

    private DoubleArrayNodePropertyValues nodePropertyValues(Optional<GraphSageResult> graphSageResult) {
        return graphSageResult
            .map(GraphSageResult::embeddings)
            .map(embeddings -> (DoubleArrayNodePropertyValues) new EmbeddingNodePropertyValues(embeddings))
            .orElse(EmptyDoubleArrayNodePropertyValues.INSTANCE);
    }

    private static class EmbeddingNodePropertyValues implements DoubleArrayNodePropertyValues {
        private final long size;
        private final HugeObjectArray<double[]> embeddings;

        EmbeddingNodePropertyValues(HugeObjectArray<double[]> embeddings) {
            this.embeddings = embeddings;
            this.size = embeddings.size();
        }

        @Override
        public long nodeCount() {
            return size;
        }

        @Override
        public double[] doubleArrayValue(long nodeId) {

            return embeddings.get(nodeId);
        }
    }
}
