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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.model.Model.CustomInfo;
import org.neo4j.gds.model.ModelConfig;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

public record ModelMetaData<CONFIG extends ModelConfig, INFO extends CustomInfo>(
    String creator,
    List<String> sharedWith,
    String name,
    ModelType algoType,
    GraphSchema graphSchema,
    CONFIG trainConfig,
    ZonedDateTime creationTime,
    INFO customInfo,
    String gdsVersion
) {
    public ModelMetaData {
        sharedWith = List.copyOf(sharedWith);
        Objects.requireNonNull(creationTime);
    }

    public static final String PUBLIC_MODEL_SUFFIX = "_public";
    private static final String ALL_USERS = "*";

    public static <C extends ModelConfig, I extends CustomInfo> ModelMetaData<C, I> of(
        String gdsVersion,
        ModelType algoType,
        GraphSchema graphSchema,
        C trainConfig,
        I customInfo
    ) {
        return new ModelMetaData<>(
            trainConfig.username(),
            List.of(),
            trainConfig.modelName(),
            algoType,
            graphSchema,
            trainConfig,
            now(),
            customInfo,
            gdsVersion
        );
    }

    public ModelMetaData<CONFIG, INFO> withCreationTime(ZonedDateTime creationTime) {
        return new ModelMetaData<>(creator, sharedWith, name, algoType, graphSchema, trainConfig, creationTime, customInfo, gdsVersion);
    }

    public ModelMetaData<CONFIG, INFO> publish() {
        var name = name() + PUBLIC_MODEL_SUFFIX;
        return new ModelMetaData<>(creator, List.of(ALL_USERS), name, algoType, graphSchema, trainConfig, creationTime, customInfo, gdsVersion);
    }

    public boolean isPublished() {
        return sharedWith.contains(ALL_USERS);
    }

    public ModelHash hash() {
        return new ModelHash(creator, name, creationTime);
    }

    private static ZonedDateTime now() {
        var zoneId = Config.EMPTY.get(GraphDatabaseSettings.db_temporal_timezone);
        return ZonedDateTime.now(Clock.system(zoneId != null ? zoneId : ZoneId.systemDefault()));
    }
}
