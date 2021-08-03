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

import org.neo4j.gds.core.model.proto.ModelProto;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoZonedDateTime;

public final class ZonedDateTimeSerializer {

    private ZonedDateTimeSerializer() {}

    public static ModelProto.ZonedDateTime toSerializable(ChronoZonedDateTime<LocalDate> zonedDateTime) {
        Instant creationTimeInstant = zonedDateTime.toInstant();
        return ModelProto.ZonedDateTime.newBuilder()
            .setSeconds(creationTimeInstant.getEpochSecond())
            .setNanos(creationTimeInstant.getNano())
            .setZoneId(zonedDateTime.getZone().getId())
            .build();
    }

    public static ZonedDateTime fromSerializable(ModelProto.ZonedDateTime protoZoneDateTime) {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(
            protoZoneDateTime.getSeconds(),
            protoZoneDateTime.getNanos()
        ), ZoneId.of(protoZoneDateTime.getZoneId()));
    }
}
