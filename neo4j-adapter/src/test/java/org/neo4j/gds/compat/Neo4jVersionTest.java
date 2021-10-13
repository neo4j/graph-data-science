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
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.kernel.internal.Version;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class Neo4jVersionTest {
    private static final String CUSTOM_VERSION_SETTING = "unsupported.neo4j.custom.version";

    @ParameterizedTest
    @CsvSource({
        "4.1, V_4_1",
        "4.1-foo, V_4_1",
        "4.1.1, V_4_1",
        "4.1.1-foo, V_4_1",
        "4.2, V_4_2",
        "4.2-foo, V_4_2",
        "4.2.1, V_4_2",
        "4.2.1-foo, V_4_2",
        "4.3.0, V_4_3"
    })
    void testParse(String input, Neo4jVersion expected) {
        assertEquals(expected.name(), Neo4jVersion.parse(input).name());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "dev",
        "4.4.0-dev",
        "4.4.0-drop03.1"
    })
    void doesntSupportDevOrLaterDropVersions(String input) {
        assertThatThrownBy(() -> Neo4jVersion.parse(input))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot run on Neo4j Version %s", input);
    }

    @Test
    void shouldNotRespectVersionOverride() {
        System.setProperty(Neo4jVersionTest.CUSTOM_VERSION_SETTING, "foobidoobie");
        assertNotEquals(Version.getNeo4jVersion(), Neo4jVersion.neo4jVersion());
    }
}
