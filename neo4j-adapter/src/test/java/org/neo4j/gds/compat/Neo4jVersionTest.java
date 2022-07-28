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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class Neo4jVersionTest {
    private static final String CUSTOM_VERSION_SETTING = CustomVersionSetting.getConfigKey();

    @ParameterizedTest
    @CsvSource({
        "4.3.0-drop04, V_4_3",
        "4.3.0, V_4_3",
        "4.4.0, V_4_4",
        "4.4.12, V_4_4",
        "4.4.8-drop01.0, V_4_4_8_drop10",
        "4.4.9-drop01.0, V_4_4_9_drop10",
        "5.0.0-drop04.0, V_5_0_drop40",
        "5.0.0-drop05.0, V_5_0_drop50"
    })
    void testParse(String input, Neo4jVersion expected) {
        assertEquals(expected.name(), Neo4jVersion.parse(input).name());
    }

    @Test
    void shouldNotRespectVersionOverride() {
        System.setProperty(Neo4jVersionTest.CUSTOM_VERSION_SETTING, "foobidoobie");
        assertNotEquals(Version.getNeo4jVersion(), Neo4jVersion.neo4jVersion());
    }
}
