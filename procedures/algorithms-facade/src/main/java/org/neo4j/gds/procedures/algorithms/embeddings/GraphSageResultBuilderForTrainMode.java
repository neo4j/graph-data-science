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
package org.neo4j.gds.procedures.algorithms.embeddings;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.model.ModelConfig;

import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;

class GraphSageResultBuilderForTrainMode implements ResultBuilder<GraphSageTrainConfig, Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>, Stream<GraphSageTrainResult>, Void> {
    @Override
    public Stream<GraphSageTrainResult> build(
        Graph graph,
        GraphSageTrainConfig configuration,
        Optional<Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>> result,
        AlgorithmProcessingTimings timings,
        Optional<Void> unused
    ) {
        return result.stream().map(model -> createResult(model, timings.computeMillis));
    }

    private static GraphSageTrainResult createResult(
        Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> trainedModel,
        long trainMillis
    ) {
        var modelInfo = new HashMap<String, Object>();
        modelInfo.put(ModelConfig.MODEL_NAME_KEY, trainedModel.name());
        modelInfo.put(ModelConfig.MODEL_TYPE_KEY, trainedModel.algoType());
        modelInfo.putAll(trainedModel.customInfo().toMap());

        var configurationMap = trainedModel.trainConfig().toMap();

        return new GraphSageTrainResult(modelInfo, configurationMap, trainMillis);
    }
}
