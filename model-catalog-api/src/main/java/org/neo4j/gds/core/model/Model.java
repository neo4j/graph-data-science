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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.ml_api.TrainingMethod;
import org.neo4j.gds.model.ModelConfig;

import java.io.Serializable;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

@ValueClass
public interface Model<DATA, CONFIG extends ModelConfig & BaseConfig, INFO extends Model.CustomInfo> {

    private static ZonedDateTime now() {
        var zoneId = Config.EMPTY.get(GraphDatabaseSettings.db_temporal_timezone);
        return ZonedDateTime.now(Clock.system(zoneId != null ? zoneId : ZoneId.systemDefault()));
    }

    String ALL_USERS = "*";
    String PUBLIC_MODEL_SUFFIX = "_public";

    String creator();

    @Value.Default
    default List<String> sharedWith() {
        return List.of();
    }

    String name();

    String algoType();

    GraphSchema graphSchema();

    @Nullable DATA data();

    CONFIG trainConfig();

    @Value.Default
    default ZonedDateTime creationTime() {
        return now();
    }

    INFO customInfo();

    Optional<Path> fileLocation();

    @Value.Derived
    default boolean loaded() {
        return data() != null;
    }

    @Value.Derived
    default boolean stored() {
        return fileLocation().isPresent();
    }

    @Value.Derived
    default boolean isPublished() {
        return sharedWith().contains(ALL_USERS);
    }

    static <D, C extends ModelConfig & BaseConfig, INFO extends CustomInfo> Model<D, C, INFO> of(
        String algoType,
        GraphSchema graphSchema,
        D modelData,
        C trainConfig,
        INFO customInfo
    ) {
        return ImmutableModel.<D, C, INFO>builder()
            .creator(trainConfig.username())
            .name(trainConfig.modelName())
            .algoType(algoType)
            .graphSchema(graphSchema)
            .data(modelData)
            .trainConfig(trainConfig)
            .customInfo(customInfo)
            .build();
    }

    interface CustomInfo extends ToMapConvertible, Serializable{
        Optional<TrainingMethod> optionalTrainerMethod();
    }
}
