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

class Neo4jVersionLookupTest {
    private static final String CUSTOM_VERSION_SETTING = CustomVersionSetting.getConfigKey();

    @Test
    void testParse() {
        assertThat(Neo4jVersionLookup.parse("5.23.0", "5.23.0"))
            .returns(new Neo4jVersion.MajorMinor(5, 23), Neo4jVersion::semanticVersion)
            .returns(true, Neo4jVersion::isSupported);
    }

    @Test
    void testParseNext() {
        assertThat(Neo4jVersionLookup.parse("5.24.0SNAPSHOT", "5.24.0-SNAPSHOT"))
            .returns(true, v -> v.matches(5, 24))
            .returns(true, Neo4jVersion::isSupported);
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
            "5.3.0", // ^
            "5.4.0", // |
            "5.5.0", // 5.x versions no longer supported
            "5.6.0", // |
            "5.7.0", // v
            "5.8.0",
            "5.9.0",
            "5.10.0",
            "5.11.0",
            "5.12.0",
            "5.13.0",
            "5.14.0",
            "dev.5.dev.1",
            "5",
            "6.0.0",
        }
    )
    void testParseInvalid(String input) {
        var version = Neo4jVersionLookup.parse(input, input);
        assertThat(version).returns(false, Neo4jVersion::isSupported);
    }

    @Test
    void shouldNotRespectVersionOverride() {
        System.setProperty(Neo4jVersionLookupTest.CUSTOM_VERSION_SETTING, "foobidoobie");
        assertThat(Neo4jVersionLookup.findNeo4jVersion().toString())
            .isNotEqualTo(Version.getNeo4jVersion());
    }

    @Test
    void semanticVersion() {
        Neo4jVersion version = Neo4jVersionLookup.parse("5.13.37", "5.13.37");

        assertThat(version.semanticVersion()).isEqualTo(new Neo4jVersion.MajorMinor(5, 13));
    }
}
