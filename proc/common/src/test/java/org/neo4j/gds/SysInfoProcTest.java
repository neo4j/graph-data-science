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

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gds.core.Settings;
import org.neo4j.kernel.internal.Version;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.condition.AnyOf.anyOf;

class SysInfoProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(SysInfoProc.class);
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder
            // add another unrestricted to test string concatenation in debug output
            .setConfig(Settings.procedureUnrestricted(), List.of("gds.*", "foo.bar"))
            .setConfig(Settings.pagecacheMemory(), "42M")
            .setConfig(
                Settings.transactionStateAllocation(),
                GraphDatabaseSettings.TransactionStateMemoryAllocation.ON_HEAP
            )
            .setConfig(Settings.transactionStateMaxOffHeapMemory(), 1337L);
    }

    @Test
    void testSysInfoProc() throws IOException {
        var result = runQuery("CALL gds.debug.sysInfo()", res -> res.stream().collect(
            toMap(m -> String.valueOf(m.get("key")), m -> m.get("value"))
        ));
        var buildInfoProperties = BuildInfoProperties.get();
        var isNotNull = new Condition<>(Objects::nonNull, "is not null");
        var openGds = new Condition<>("OpenGDS"::equals, "OpenGDS");
        var isTrue = new Condition<>(Boolean.TRUE::equals, "true");
        var isFalse = new Condition<>(Boolean.FALSE::equals, "false");
        var isInteger = new Condition<>(v -> (v instanceof Long) || (v instanceof Integer), "isInteger");
        var maxOffHeap40 = new Condition<>("dbms.tx_state.max_off_heap_memory"::equals, "max off-heap setting on 4.0");
        var maxOffHeap41 = new Condition<>("dbms.memory.off_heap.max_size"::equals, "max off-heap setting on 4.1");
        var is1337 = new Condition<>(Long.valueOf(1337L)::equals, "1337");

        assertThat(result)
            .hasSizeGreaterThanOrEqualTo(42) // this is actually the min number of entries :)
            .containsEntry("gdsVersion", buildInfoProperties.gdsVersion())
            .containsEntry("minimumRequiredJavaVersion", buildInfoProperties.minimumRequiredJavaVersion())
            .containsEntry("buildDate", buildInfoProperties.buildDate())
            .containsEntry("buildJdk", buildInfoProperties.buildJdk())
            .containsEntry("buildJavaVersion", buildInfoProperties.buildJavaVersion())
            .containsEntry("buildHash", buildInfoProperties.buildHash())
            .containsEntry("neo4jVersion", Version.getNeo4jVersion())
            .hasEntrySatisfying("gdsEdition", openGds)
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
            .hasEntrySatisfying("containerized", anyOf(isTrue, isFalse))
            .containsEntry("dbms.security.procedures.unrestricted", "gds.*,foo.bar")
            .containsEntry("dbms.memory.pagecache.size", "42M")
            .containsEntry("dbms.tx_state.memory_allocation", "ON_HEAP")
            .hasEntrySatisfying(anyOf(maxOffHeap40, maxOffHeap41), is1337);
    }

    @Test
    void shouldReturnGradleVersion() throws IOException {
        var result = runQuery(
            "CALL gds.debug.sysInfo() YIELD key, value WITH key, value WHERE key = 'gdsVersion' RETURN value as gdsVersion",
            cypherResult -> cypherResult.<String>columnAs("gdsVersion").stream().collect(toList())
        );
        assertThat(result).containsExactly(BuildInfoProperties.get().gdsVersion());
    }
}
