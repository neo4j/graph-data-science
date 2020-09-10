/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.internal.Version;

import java.io.IOException;
import java.util.Objects;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.condition.AnyOf.anyOf;

class DebugProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(DebugProc.class);
    }

    @Test
    void testDebugProc() throws IOException {
        var result = runQuery("CALL gds.debug()", res -> res.stream().collect(
            toMap(m -> String.valueOf(m.get("key")), m -> m.get("value"))
        ));
        var buildInfoProperties = BuildInfoProperties.get();
        var isNotNull = new Condition<>(Objects::nonNull, "is not null");
        var community = new Condition<>("Community"::equals, "Community");
        var enterprise = new Condition<>("Enterprise"::equals, "Enterprise");
        var invalidLicense = new Condition<>("Enterprise (invalid license)"::equals, "Enterprise (invalid license)");
        var isTrue = new Condition<>(Boolean.TRUE::equals, "true");
        var isFalse = new Condition<>(Boolean.FALSE::equals, "false");
        var isInteger = new Condition<>(v -> (v instanceof Long) || (v instanceof Integer), "isInteger");

        assertThat(result)
            .hasSizeGreaterThanOrEqualTo(38)
            .containsEntry("gdsVersion", buildInfoProperties.gdsVersion())
            .containsEntry("minimumRequiredJavaVersion", buildInfoProperties.minimumRequiredJavaVersion())
            .containsEntry("buildDate", buildInfoProperties.buildDate())
            .containsEntry("buildJdk", buildInfoProperties.buildJdk())
            .containsEntry("buildJavaVersion", buildInfoProperties.buildJavaVersion())
            .containsEntry("buildHash", buildInfoProperties.buildHash())
            .containsEntry("neo4jVersion", Version.getNeo4jVersion())
            .hasEntrySatisfying("gdsEdition", anyOf(enterprise, community, invalidLicense))
            .hasEntrySatisfying("availableCPUs", isInteger)
            .hasEntrySatisfying("physicalCPUs", isInteger)
            .hasEntrySatisfying("availableHeapInBytes", isInteger)
            .hasEntrySatisfying("availableHeap", isNotNull)
            .hasEntrySatisfying("heapFreeInBytes", isInteger)
            .hasEntrySatisfying("heapFree", isNotNull)
            .hasEntrySatisfying("heapTotalInBytes", isInteger)
            .hasEntrySatisfying("heapTotal", isNotNull)
            .hasEntrySatisfying("heapMaxInBytes", isInteger)
            .hasEntrySatisfying("heapMax", isNotNull)
            .hasEntrySatisfying("offHeapUsedInBytes", isInteger)
            .hasEntrySatisfying("offHeapUsed", isNotNull)
            .hasEntrySatisfying("offHeapTotalInBytes", isInteger)
            .hasEntrySatisfying("offHeapTotal", isNotNull)
            .hasEntrySatisfying("freePhysicalMemoryInBytes", isInteger)
            .hasEntrySatisfying("freePhysicalMemory", isNotNull)
            .hasEntrySatisfying("committedVirtualMemoryInBytes", isInteger)
            .hasEntrySatisfying("committedVirtualMemory", isNotNull)
            .hasEntrySatisfying("totalPhysicalMemoryInBytes", isInteger)
            .hasEntrySatisfying("totalPhysicalMemory", isNotNull)
            .hasEntrySatisfying("freeSwapSpaceInBytes", isInteger)
            .hasEntrySatisfying("freeSwapSpace", isNotNull)
            .hasEntrySatisfying("totalSwapSpaceInBytes", isInteger)
            .hasEntrySatisfying("totalSwapSpace", isNotNull)
            .hasEntrySatisfying("openFileDescriptors", isInteger)
            .hasEntrySatisfying("maxFileDescriptors", isInteger)
            .hasEntrySatisfying("vmName", isNotNull)
            .hasEntrySatisfying("vmVersion", isNotNull)
            .hasEntrySatisfying("vmCompiler", isNotNull)
            .hasEntrySatisfying("containerized", anyOf(isTrue, isFalse));
    }

    @Test
    void shouldReturnGradleVersion() throws IOException {
        var result = runQuery(
            "CALL gds.debug() YIELD key, value WITH key, value WHERE key = 'gdsVersion' RETURN value as gdsVersion",
            cypherResult -> cypherResult.<String>columnAs("gdsVersion").stream().collect(toList())
        );
        assertThat(result).containsExactly(BuildInfoProperties.get().gdsVersion());
    }
}
