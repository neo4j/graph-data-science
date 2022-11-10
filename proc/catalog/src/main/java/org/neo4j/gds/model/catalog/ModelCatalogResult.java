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
package org.neo4j.gds.model.catalog;

import org.neo4j.gds.core.model.Model;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public class ModelCatalogResult {
    public final Map<String, Object> modelInfo;
    public final Map<String, Object> trainConfig;
    public final Map<String, Object> graphSchema;
    public final boolean loaded;
    public final boolean stored;
    public final ZonedDateTime creationTime;
    public final boolean shared;

    public ModelCatalogResult(Model<?, ?, ?> model) {
        shared = !model.sharedWith().isEmpty();
        modelInfo = Stream.concat(
            Map.of(
                "modelName", model.name(),
                "modelType", model.algoType()
            ).entrySet().stream(),
            model.customInfo().toMap().entrySet().stream()
        ).collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            )
        );

        trainConfig = model.trainConfig().toMap();
        graphSchema = model.graphSchema().toMapOld();
        loaded = model.loaded();
        stored = model.stored();
        creationTime = model.creationTime();
    }
}
