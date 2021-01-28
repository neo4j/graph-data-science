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
package org.neo4j.graphalgo.compat;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.kernel.internal.Version;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Neo4jVersionTest {

    @ParameterizedTest
    @CsvSource({
        "4.0, V_4_0",
        "4.0-foo, V_4_0",
        "4.0.2, V_4_0",
        "4.0.2-foo, V_4_0",
        "4.1, V_4_1",
        "4.1-foo, V_4_1",
        "4.1.1, V_4_1",
        "4.1.1-foo, V_4_1",
        "4.2, V_4_2",
        "4.2-foo, V_4_2",
        "4.2.1, V_4_2",
        "4.2.1-foo, V_4_2",
        "dev, V_4_2",
        "aura, V_4_2",
    })
    void testParse(CharSequence input, Neo4jVersion expected) {
        assertEquals(expected, Neo4jVersion.parse(input));
    }

    @Test
    void defaultToKernelVersion() {
        assertEquals(Version.getKernel().getReleaseVersion(), Neo4jVersion.neo4jVersion());
    }

    @Test
    void hasSystemPropertyOverride() {
        var version = "1.3.3.7";
        withSystemProperty(
            Neo4jVersion.OVERRIDE_VERSION_PROPERTY,
            version,
            () -> assertEquals(version, Neo4jVersion.neo4jVersion())
        );
    }

    @Test
    @Disabled("the Version from Neo4j is initialized once on JVM startup, " +
              "not on every check. It also stores the result in a static final field. " +
              "We would need to perform some magic, either mocking, heavy reflection, forking a new JVM, etc" +
              "For now, let's disable this as we don't _really_ need to test Neo4j functionality")
    void hasNeo4jSystemPropertyOverride() {
        var version = "2.1.8.4";
        withSystemProperty(
            "unsupported.neo4j.custom.version",
            version,
            () -> assertEquals(version, Neo4jVersion.neo4jVersion())
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"4.0-aura", "4.0.aura", "4.2-aura", "4.2.aura", "4.0-AuraFoo", "4.0-AuraBar"})
    void recognizesAuraVersion(String version) {
        withSystemProperty(
            Neo4jVersion.OVERRIDE_VERSION_PROPERTY,
            version,
            () -> assertEquals("aura", Neo4jVersion.neo4jVersion())
        );
    }

    private void withSystemProperty(String name, String value, Runnable test) {
        var old = System.setProperty(name, value);
        try {
            test.run();
        } finally {
            if (old == null) {
                System.clearProperty(name);
            } else {
                System.setProperty(name, old);
            }
        }
    }
}
