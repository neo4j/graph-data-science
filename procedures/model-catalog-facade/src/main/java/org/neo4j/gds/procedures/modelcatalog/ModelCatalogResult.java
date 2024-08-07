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
package org.neo4j.gds.procedures.modelcatalog;

import org.neo4j.gds.core.model.Model;

import java.time.ZonedDateTime;
import java.util.Map;

public class ModelCatalogResult {
    public final String modelName;
    public final String modelType;
    public final Map<String, Object> modelInfo;
    public final ZonedDateTime creationTime;
    public final Map<String, Object> trainConfig;
    public final Map<String, Object> graphSchema;
    public final boolean loaded;
    public final boolean stored;
    public final boolean published;

    public ModelCatalogResult(Model<?, ?, ?> model) {
        this.modelName = model.name();
        this.modelType = model.algoType();
        this.modelInfo = model.customInfo().toMap();
        this.creationTime = model.creationTime();
        this.trainConfig = model.trainConfig().toMap();
        this.graphSchema = model.graphSchema().toMapOld();
        this.loaded = model.loaded();
        this.stored = model.stored();
        this.published = !model.sharedWith().isEmpty();
    }
}
