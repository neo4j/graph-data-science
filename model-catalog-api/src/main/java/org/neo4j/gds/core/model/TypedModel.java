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

import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.model.ModelConfig;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public record TypedModel<D, C extends ModelConfig, I extends CustomInfo>(
    Model model,
    D data,
    C trainConfig,
    I customInfo
) {
    public static <D, C extends ModelConfig, I extends CustomInfo> TypedModel<D, C, I> of(
        Model model, Class<D> dataClass, Class<C> configClass, Class<I> infoClass
    ) {
        var message = "The model `%1$s` has %2$s with different type than expected. " +
            "Expected %2$s type: `%3$s`, invoked with %2$s type: `%4$s`.";
        if (!dataClass.isInstance(model.data())) {
            throw new IllegalArgumentException(formatWithLocale(
                message, model.name(), "data", dataClass.getName(), model.data().getClass().getName()
            ));
        }
        var data = (D) model.data();
        if (!configClass.isInstance(model.trainConfig())) {
            throw new IllegalArgumentException(formatWithLocale(
                message, model.name(), "trainConfig", configClass.getName(), model.trainConfig().getClass().getName()
            ));
        }
        var config = (C) model.trainConfig();
        if (!infoClass.isInstance(model.customInfo())) {
            throw new IllegalArgumentException(formatWithLocale(
                message, model.name(), "customInfo", infoClass.getName(), model.customInfo().getClass().getName()
            ));
        }
        var info = (I) model.customInfo();

        return new TypedModel<D, C, I>(model, data, config, info);
    }

    public static <D, C extends ModelConfig, I extends CustomInfo> TypedModel<D, C, I> trained(
        String gdsVersion, ModelType modelType, GraphSchema schema, D data, C config, I info
    ) {
        var metadata = ModelMetaData.of(gdsVersion, modelType, schema, config, info);
        return new TypedModel<>(Model.trained(metadata, data), data, config, info);
    }

    public static <D, C extends ModelConfig, I extends CustomInfo> TypedModel<D, C, I> trained(
        ModelMetaData<C, I> metadata, D data
    ) {
        return new TypedModel<>(Model.trained(metadata, data), data, metadata.trainConfig(), metadata.customInfo());
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

    public ModelMetaData metaData()     { return model.metaData(); }

    public String creator()              { return model.creator(); }
    public String name()                 { return model.name(); }
    public ModelType algoType()          { return model.algoType(); }
    public GraphSchema graphSchema()     { return model.graphSchema(); }
    public String gdsVersion()           { return model.gdsVersion(); }
    public ZonedDateTime creationTime()  { return model.creationTime(); }
    public boolean isPublished()         { return model.isPublished(); }
    public ModelHash hash()              { return model.hash(); }

    public boolean isStored()             { return model.isStored(); }
}
