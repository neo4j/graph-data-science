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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.kernel.internal.CustomVersionSetting;
import org.neo4j.kernel.internal.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class Neo4jVersionTest {
    private static final String CUSTOM_VERSION_SETTING = CustomVersionSetting.getConfigKey();

    @ParameterizedTest
    @CsvSource({
        "5.14.0, V_5_14",
        "5.15.0, V_5_15",
        "5.16.0, V_5_16",
        "5.17.0, V_5_17",
        "5.18.1, V_5_18",
        "5.19.0, V_5_19",
        "5.20.0, V_5_20",
        "5.21.0, V_Dev",
    })
    void testParse(String input, Neo4jVersion expected) {
        assertEquals(expected.name(), Neo4jVersion.parse(input).name());
    }

    @ParameterizedTest
    @CsvSource(
        {
            "dev",
            "4.3", // EOL
            "5.dev",
            "dev.5",
            "5.0", // 5.0 was never released to the public
            "5.1.0",
            "5.2.0",
            "5.2.0",
            "5.3.0",
            "5.4.0",
            "5.5.0", // 5.x versions no longer supported
            "5.6.0",
            "5.7.0",
            "5.8.0",
            "5.9.0",
            "5.10.0",
            "dev.5.dev.1",
            "5",
            "6.0.0",
        })
    void testParseInvalid(String input) {
        assertThatThrownBy(() -> Neo4jVersion.parse(input))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot run on Neo4j Version " + input);
    }

    @Test
    void shouldNotRespectVersionOverride() {
        System.setProperty(Neo4jVersionTest.CUSTOM_VERSION_SETTING, "foobidoobie");
        assertNotEquals(Version.getNeo4jVersion(), Neo4jVersion.neo4jVersion());
    }

    @ParameterizedTest
    @CsvSource(
        {
            "5.14.0, 5, 14",
            "5.15.0, 5, 15",
            "5.16.0, 5, 16",
            "5.17.0, 5, 17",
            "5.18.1, 5, 18",
            "5.19.0, 5, 19",
            "5.20.0, 5, 20",
        }
    )
    void semanticVersion(String input, int expectedMajor, int expectedMinor) {
        Neo4jVersion version = Neo4jVersion.parse(input);

        assertThat(version.semanticVersion()).isEqualTo(ImmutableMajorMinorVersion.of(expectedMajor, expectedMinor));
    }
}
