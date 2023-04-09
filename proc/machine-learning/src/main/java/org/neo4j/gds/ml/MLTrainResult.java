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
package org.neo4j.gds.ml;

import org.neo4j.gds.core.model.Model;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;
import static org.neo4j.gds.model.ModelConfig.MODEL_TYPE_KEY;

@SuppressWarnings({"unused", "WeakerAccess"})
public class MLTrainResult {

    public final long trainMillis;
    public final Map<String, Object> modelInfo;
    public final Map<String, Object> configuration;

    public MLTrainResult(
        Optional<Model<?, ?, ?>> maybeTrainedModel,
        long trainMillis
    ) {
        if(maybeTrainedModel.isPresent()) {
            var trainedModel = maybeTrainedModel.get();
            this.modelInfo = Stream.concat(
                Map.of(
                    MODEL_NAME_KEY, trainedModel.name(),
                    MODEL_TYPE_KEY, trainedModel.algoType()
                ).entrySet().stream(),
                trainedModel.customInfo().toMap().entrySet().stream()
            ).collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue)
            );
            this.configuration = trainedModel.trainConfig().toMap();
        } else {
            modelInfo = Collections.emptyMap();
            configuration = Collections.emptyMap();
        }
        this.trainMillis = trainMillis;
    }
}
