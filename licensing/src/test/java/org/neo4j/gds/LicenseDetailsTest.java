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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class LicenseDetailsTest {
    @Test
    void unlicensed() {
        assertThat(LicenseDetails.from(new TestLicenseStates.Unlicensed()))
            .satisfies(res -> assertThat(res.isLicensed()).isFalse())
            .satisfies(res -> assertThat(res.details()).isEqualTo("No valid GDS license specified."));
    }

    @Test
    void invalid() {
        assertThat(LicenseDetails.from(new TestLicenseStates.Invalid()))
            .satisfies(res -> assertThat(res.isLicensed()).isFalse())
            .satisfies(res -> assertThat(res.details()).isEqualTo("License error: License invalid for some reason."));
    }

    @Test
    void expired() {
        assertThat(LicenseDetails.from(new TestLicenseStates.ExpiredLicenseState(ZonedDateTime.ofInstant(Instant.ofEpochSecond(0), ZoneId.of("UTC")))))
            .satisfies(res -> assertThat(res.isLicensed()).isFalse())
            .satisfies(res -> assertThat(res.details()).isEqualTo("License error: License expires a long time ago. Expiration date at License expires at 1970-01-01T00:00Z[UTC]"));
    }

    @Test
    void valid() {
        assertThat(LicenseDetails.from(new TestLicenseStates.Valid(ZonedDateTime.ofInstant(Instant.ofEpochSecond(99999999999L), ZoneId.of("UTC")))))
            .satisfies(res -> assertThat(res.isLicensed()).isTrue())
            .satisfies(res -> assertThat(res.details()).isEqualTo("License expires at 5138-11-16T09:46:39Z[UTC]"));
    }
}
