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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.core.model.CustomInfo;
import org.neo4j.gds.core.model.ModelType;
import org.neo4j.gds.model.ModelConfig;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface MLTrainResult {
    long trainMillis();
    Map<String, Object> modelInfo();
    Map<String, Object> configuration();

    static Map<String, Object> createModelInfo(String modelName, ModelType modelType, CustomInfo customInfo) {
        return Stream.concat(
                Map.of(
                        ModelConfig.MODEL_NAME_KEY, modelName,
                        ModelConfig.MODEL_TYPE_KEY, modelType.value()
                ).entrySet().stream(),
                customInfo.toMap().entrySet().stream()
        ).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                )
        );
    }
}
