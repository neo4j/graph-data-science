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
package org.neo4j.gds.compat;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.internal.Version;

import static org.assertj.core.api.Assertions.assertThat;

class Neo4jVersionLookupTest {

    @Test
    void testParseCalVer() {
        assertThat(Neo4jVersionLookup.parse("2025.04.0", "2025.04.0"))
            .hasToString("2025.4")
            .returns(true, v -> v.matches(2025, 4));
    }

    @Test
    void testParseV5() {
        assertThat(Neo4jVersionLookup.parse("5.26.9", "5.26.9"))
            .returns(true, v -> v.matches(5, 26));
    }

    @Test
    void testParseDevbuild() {
        String fullVersion = "2025.04.0.dev";
        Neo4jVersion version = Neo4jVersionLookup.parse("2025.04.0", fullVersion);
        assertThat(version.fullVersion()).isEqualTo(fullVersion);
        assertThat(version.matches(2025, 4)).isTrue();
    }

    @Test
    void parseInvalid() {
        String fullVersion = "NOT_A_VERSION";
        Neo4jVersion version = Neo4jVersionLookup.parse(fullVersion, fullVersion);
        assertThat(version.fullVersion()).isEqualTo(fullVersion);
    }

    @Test
    void parsePackagedVersion() {
        String fullVersion = Version.class.getPackage().getImplementationVersion();
        assertThat(Neo4jVersionLookup.neo4jVersion().fullVersion()).isEqualTo(fullVersion);
    }

}
