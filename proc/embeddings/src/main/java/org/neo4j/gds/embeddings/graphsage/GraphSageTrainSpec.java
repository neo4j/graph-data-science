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

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrain;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.embeddings.GraphSageTrainResult;

import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.GRAPH_SAGE_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.TRAIN;

@GdsCallable(name = "gds.beta.graphSage.train", description = GRAPH_SAGE_DESCRIPTION, executionMode = TRAIN)
public class GraphSageTrainSpec implements AlgorithmSpec<
    GraphSageTrain,
    Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>,
    GraphSageTrainConfig,
    Stream<GraphSageTrainResult>,
    GraphSageTrainAlgorithmFactory> {
    @Override
    public String name() {
        return "GraphSageTrain";
    }

    @Override
    public GraphSageTrainAlgorithmFactory algorithmFactory(ExecutionContext executionContext) {
        return new GraphSageTrainAlgorithmFactory();
    }

    @Override
    public NewConfigFunction<GraphSageTrainConfig> newConfigFunction() {
        return GraphSageTrainConfig::of;
    }

    @Override
    public ComputationResultConsumer<GraphSageTrain, Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>, GraphSageTrainConfig, Stream<GraphSageTrainResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
