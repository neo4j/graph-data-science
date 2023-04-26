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
package org.neo4j.gds;

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.model.ModelConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;
import static org.neo4j.gds.model.ModelConfig.MODEL_TYPE_KEY;

// FIXME replace this with MLTrainResult (duplicate?) --> VN: this would require to add dependency to `:proc-machine-learning` which is not good...
public class TrainResult {

    public final Map<String, Object> modelInfo;
    public final Map<String, Object> configuration;
    public final long trainMillis;

    public <TRAIN_RESULT, TRAIN_CONFIG extends ModelConfig & AlgoBaseConfig, TRAIN_INFO extends Model.CustomInfo> TrainResult(
        Optional<Model<TRAIN_RESULT, TRAIN_CONFIG, TRAIN_INFO>> maybeTrainedModel,
        long trainMillis
    ) {
        this.modelInfo = new HashMap<>();
        this.configuration = new HashMap<>();

        maybeTrainedModel.ifPresent(trainedModel -> {
            TRAIN_CONFIG trainConfig = trainedModel.trainConfig();

            modelInfo.put(MODEL_NAME_KEY, trainedModel.name());
            modelInfo.put(MODEL_TYPE_KEY, trainedModel.algoType());
            modelInfo.putAll(trainedModel.customInfo().toMap());
            configuration.putAll(trainConfig.toMap());
        });

        this.trainMillis = trainMillis;
    }
}
