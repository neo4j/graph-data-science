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

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ModelMetadataTest {

    @Test
    void shouldNormalizeZoneOffsetUtcToZoneIdUtc() {
        var createdAt = ZonedDateTime.parse("2026-08-27T20:30:55Z");
        assertThat(createdAt.getZone()).isEqualTo(ZoneOffset.UTC);

        var metadata = new ModelMetadata(
            "m", "user", "graphSage",
            createdAt,
            Map.of(), Map.of(), Map.of(),
            false, true, false
        );

        assertThat(metadata.creationTime().getZone()).isEqualTo(ZoneId.of("UTC"));
        assertThat(metadata.creationTime().toInstant()).isEqualTo(createdAt.toInstant());
    }

    @Test
    void shouldPreserveNonUtcZone() {
        var berlin = ZonedDateTime.parse("2025-01-02T12:39:46.745137+01:00[Europe/Berlin]");

        var metadata = new ModelMetadata(
            "m", "user", "graphSage",
            berlin,
            Map.of(), Map.of(), Map.of(),
            false, true, false
        );

        assertThat(metadata.creationTime().getZone()).isEqualTo(ZoneId.of("Europe/Berlin"));
        assertThat(metadata.creationTime().toInstant()).isEqualTo(berlin.toInstant());
    }
}
