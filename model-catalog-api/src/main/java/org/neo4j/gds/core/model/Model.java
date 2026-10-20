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
package org.neo4j.gds.core.model;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.model.ModelConfig;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

/**
 * A Model can be created in three ways.
 * 1. It can be the product of training. In this case it has Metadata and Data but no FileLocation.
 * 2. It can be retrieved from the ModelCatalog. In this case it may be retrieved with or without Data.
 *    It may also be stored and have a FileLocation.
 * 3. It may be read from storage. In this case it will have MetaData and FileLocation.
 *    It may be retrieved with or without Data.
 */
public record Model<DATA, CONFIG extends ModelConfig, INFO extends CustomInfo>(
    ModelMetaData<CONFIG, INFO> metaData,
    @Nullable DATA data,
    Optional<Path> fileLocation
) {
    public static <D, C extends ModelConfig, I extends CustomInfo> Model<D, C, I> trained(
        ModelMetaData<C, I> metaData,
        D data
    ) {
        return new Model<>(metaData, data, Optional.empty());
    }

    public static <D, C extends ModelConfig, I extends CustomInfo> Model<D, C, I> stored(
        ModelMetaData<C, I> metaData,
        D data,
        Path fileLocation
    ) {
        return new Model<>(metaData, data, Optional.of(fileLocation));
    }

    public static <C extends ModelConfig, I extends CustomInfo> Model<Object, C, I> stored(
        ModelMetaData<C, I> metaData,
        Path fileLocation
    ) {
        return new Model<>(metaData, null, Optional.of(fileLocation));
    }

    public static <C extends ModelConfig, I extends CustomInfo> Model<Object, C, I> metadataOnly(
        ModelMetaData<C, I> metaData
    ) {
        return new Model<>(metaData, null, Optional.empty());
    }

    public <D, C extends ModelConfig, I extends CustomInfo> TypedModel<D, C, I> as(Class<D> dataClass, Class<C> configClass, Class<I> infoClass) {
        return TypedModel.of(this, dataClass, configClass, infoClass);
    }

    public <D> Model<D, CONFIG, INFO> withData(@Nullable D data) {
        return new Model<>(metaData, data, fileLocation);
    }

    public Model<DATA, CONFIG, INFO> withFileLocation(Optional<Path> fileLocation) {
        return new Model<>(metaData, data, fileLocation);
    }

    public boolean isLoaded() { return data != null; }
    public boolean isStored() { return fileLocation.isPresent(); }

    public Model<DATA, CONFIG, INFO> publish() {
        return new Model<>(metaData.publish(), data, fileLocation);
    }

    public String creator()              { return metaData.creator(); }
    public String name()                 { return metaData.name(); }
    public ModelType algoType()          { return metaData.algoType(); }
    public GraphSchema graphSchema()     { return metaData.graphSchema(); }
    public CONFIG trainConfig()          { return metaData.trainConfig(); }
    public INFO customInfo()             { return metaData.customInfo(); }
    public String gdsVersion()           { return metaData.gdsVersion(); }
    public List<String> sharedWith()     { return metaData.sharedWith(); }
    public ZonedDateTime creationTime()  { return metaData.creationTime(); }
    public boolean isPublished()         { return metaData.isPublished(); }
    public ModelKey key()                { return metaData.key(); }

    @TestOnly
    public static <D, C extends ModelConfig, I extends CustomInfo> Model<D, C, I> of(
        ModelType algoType,
        GraphSchema graphSchema,
        D data,
        C trainConfig,
        I customInfo
    ) {
        return new Model<>(
            ModelMetaData.of("default", algoType, graphSchema, trainConfig, customInfo),
            data,
            Optional.empty()
        );
    }
}
