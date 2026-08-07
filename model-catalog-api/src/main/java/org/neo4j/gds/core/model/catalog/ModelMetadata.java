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
package org.neo4j.gds.core.model.catalog;

import org.neo4j.gds.core.model.Model;

import java.time.ZonedDateTime;
import java.util.Map;

/**
 * Everything that can be reported about a model without its data.
 * Used by {@link org.neo4j.gds.core.model.ModelCatalog#getAllMetadata()} so callers can list
 * and filter models without materialising full {@link Model} objects.
 */
public record ModelMetadata(
    String name,
    String creator,
    String modelType,
    ZonedDateTime creationTime,
    Map<String, Object> graphSchema,
    Map<String, Object> trainConfig,
    Map<String, Object> modelInfo,
    boolean loaded,
    boolean stored,
    boolean published
) {

    public static ModelMetadata of(Model<?, ?, ?> model) {
        return new ModelMetadata(
            model.name(),
            model.creator(),
            model.algoType(),
            model.creationTime(),
            model.graphSchema().toMapOld(),
            model.trainConfig().toMap(),
            model.customInfo().toMap(),
            model.loaded(),
            model.stored(),
            !model.sharedWith().isEmpty()
        );
    }
}
