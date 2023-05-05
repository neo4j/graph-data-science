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

import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;

import java.util.HashMap;
import java.util.Map;

import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;
import static org.neo4j.gds.model.ModelConfig.MODEL_TYPE_KEY;

public class TrainResult {

    public final Map<String, Object> modelInfo;
    public final Map<String, Object> configuration;
    public final long trainMillis;

    TrainResult(
        Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> trainedModel,
        long trainMillis
    ) {
        var trainConfig = trainedModel.trainConfig();

        this.modelInfo = new HashMap<>();
        modelInfo.put(MODEL_NAME_KEY, trainedModel.name());
        modelInfo.put(MODEL_TYPE_KEY, trainedModel.algoType());
        modelInfo.putAll(trainedModel.customInfo().toMap());
        configuration = trainConfig.toMap();

        this.trainMillis = trainMillis;
    }
}
