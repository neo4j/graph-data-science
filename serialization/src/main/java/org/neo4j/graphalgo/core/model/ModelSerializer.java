/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.core.model;

import com.google.protobuf.ByteString;
import org.neo4j.graphalgo.core.model.proto.ModelProto;
import org.neo4j.graphalgo.utils.serialization.ObjectSerializer;

import java.io.IOException;
import java.time.Instant;

public final class ModelSerializer {

    private ModelSerializer() {}

    public static ModelProto.Model serializableFormatOf(Model<?, ?, ?> model) throws IOException {
        Instant creationTimeInstant = model.creationTime().toInstant();
        ModelProto.ZonedDateTime serializableCreationTime = ModelProto.ZonedDateTime.newBuilder()
            .setSeconds(creationTimeInstant.getEpochSecond())
            .setNanos(creationTimeInstant.getNano())
            .setZoneId(model.creationTime().getZone().getId())
            .build();
        return ModelProto.Model.newBuilder()
            .setUsername(model.username())
            .setName(model.name())
            .setAlgoType(model.algoType())
            .setSerializedTrainConfig(ByteString.copyFrom(ObjectSerializer.toByteArray(model.trainConfig())))
            .setCreationTime(serializableCreationTime)
            .build();
    }
}
