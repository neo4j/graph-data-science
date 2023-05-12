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

import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.executor.validation.ValidationConfiguration;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.GRAPH_SAGE_DESCRIPTION;
import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.nodePropertyValues;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;

@GdsCallable(name = "gds.beta.graphSage.write", description = GRAPH_SAGE_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class GraphSageWriteSpec implements AlgorithmSpec<GraphSage, GraphSageResult, GraphSageWriteConfig, Stream<WriteResult>, GraphSageAlgorithmFactory<GraphSageWriteConfig>> {

    @Override
    public String name() {
        return "GraphSageWrite";
    }

    @Override
    public GraphSageAlgorithmFactory<GraphSageWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new GraphSageAlgorithmFactory<>(executionContext.modelCatalog());
    }

    @Override
    public NewConfigFunction<GraphSageWriteConfig> newConfigFunction() {
        return GraphSageWriteConfig::of;
    }

    @Override
    public ComputationResultConsumer<GraphSage, GraphSageResult, GraphSageWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            this::nodePropertyList,
            name()
        );
    }

    @Override
    public ValidationConfiguration<GraphSageWriteConfig> validationConfig(ExecutionContext executionContext) {
        return new GraphSageConfigurationValidation<>(executionContext.modelCatalog());
    }

    private WriteResult.Builder resultBuilder(
        ComputationResult<GraphSage, GraphSageResult, GraphSageWriteConfig> computationResult,
        ExecutionContext executionContext
    ) {
        return new WriteResult.Builder();
    }

    private List<NodeProperty> nodePropertyList(ComputationResult<GraphSage, GraphSageResult, GraphSageWriteConfig> computationResult) {
        return List.of(NodeProperty.of(
            computationResult.config().writeProperty(),
            nodePropertyValues(computationResult.result())
        ));
    }
}
