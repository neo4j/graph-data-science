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

import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;

import java.util.HashMap;
import java.util.Map;

import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;
import static org.neo4j.gds.model.ModelConfig.MODEL_TYPE_KEY;

public final class GraphSageTrainResult {
    public final Map<String, Object> modelInfo;
    public final Map<String, Object> configuration;
    public final long trainMillis;

    private GraphSageTrainResult(Map<String, Object> modelInfo, Map<String, Object> configuration, long trainMillis) {
        this.modelInfo = modelInfo;
        this.configuration = configuration;
        this.trainMillis = trainMillis;
    }

    public static GraphSageTrainResult create(
        Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> trainedModel,
        long trainMillis
    ) {
        var modelInfo = new HashMap<String, Object>();
        modelInfo.put(MODEL_NAME_KEY, trainedModel.name());
        modelInfo.put(MODEL_TYPE_KEY, trainedModel.algoType());
        modelInfo.putAll(trainedModel.customInfo().toMap());

        var configurationMap = trainedModel.trainConfig().toMap();

        return new GraphSageTrainResult(modelInfo, configurationMap, trainMillis);
    }
}
