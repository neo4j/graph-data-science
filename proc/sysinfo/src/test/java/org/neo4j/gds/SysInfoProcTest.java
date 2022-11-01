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
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.Settings;
import org.neo4j.kernel.internal.Version;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.condition.AnyOf.anyOf;
import static org.neo4j.gds.GdsEditionTestCondition.GDS_EDITION;

class SysInfoProcTest extends BaseProcTest {

    private static final Collection<String> ALL_COMPATIBILITIES = List.of(
        "Neo4j 4.3",
        "Neo4j 4.4",
        "Neo4j Settings 4.x",

        "Neo4j 5.1",
        "Neo4j 5.1 (placeholder)",
        "Neo4j Settings 5.1",
        "Neo4j Settings 5.1 (placeholder)",

        "Neo4j 5.2.0-dev",
        "Neo4j 5.2.0-dev (placeholder)",
        "Neo4j Settings 5.2.0-dev",
        "Neo4j Settings 5.2.0-dev (placeholder)"
    );

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
            .setConfig(Settings.pageCacheMemory(), Settings.pageCacheMemoryValue("42M"))
            .setConfig(
                Settings.transactionStateAllocation(),
                GraphDatabaseSettings.TransactionStateMemoryAllocation.ON_HEAP
            )
            .setConfig(Settings.transactionStateMaxOffHeapMemory(), 1337L);
    }

    @Test
    void testSysInfoProc() throws IOException {
        var result = runQuery("CALL gds.debug.sysInfo()", res -> res.stream().collect(
            toMap(m -> String.valueOf(m.get("key")), m -> m.get("value"), (lhs, rhs) -> {
                if (lhs instanceof List) {
                    // noinspection unchecked
                    ((List<Object>) lhs).add(rhs);
                    return lhs;
                }

                var newLhs = new ArrayList<>();
                newLhs.add(lhs);
                newLhs.add(rhs);
                return newLhs;
            })
        ));
        var buildInfoProperties = BuildInfoProperties.get();
        var isNotNull = new Condition<>(Objects::nonNull, "is not null");
        var isTrue = new Condition<>(Boolean.TRUE::equals, "true");
        var isFalse = new Condition<>(Boolean.FALSE::equals, "false");
        var isInteger = new Condition<>(v -> (v instanceof Long) || (v instanceof Integer), "isInteger");

        var neo4jVersion = GraphDatabaseApiProxy.neo4jVersion();

        Set<String> expectedCompatibilities;
        switch (neo4jVersion) {
            case V_4_3:
                expectedCompatibilities = Set.of("Neo4j 4.3", "Neo4j Settings 4.x");
                break;
            case V_4_4:
                expectedCompatibilities = Set.of("Neo4j 4.4", "Neo4j Settings 4.x");
                break;
            case V_5_1:
                expectedCompatibilities = Set.of(
                    "Neo4j Settings 5.1 (placeholder)",
                    "Neo4j Settings 5.1",
                    "Neo4j 5.1 (placeholder)",
                    "Neo4j 5.1"
                );
                break;
            case V_Dev:
                expectedCompatibilities = Set.of(
                    "Neo4j Settings 5.2.0-dev (placeholder)",
                    "Neo4j Settings 5.2.0-dev",
                    "Neo4j 5.2.0-dev (placeholder)",
                    "Neo4j 5.2.0-dev"
                );
                break;
            default:
                throw new IllegalStateException("Unexpected Neo4j version: " + neo4jVersion);
        }

        var allCompatibilities = new HashSet<>(ALL_COMPATIBILITIES);
        allCompatibilities.removeAll(expectedCompatibilities);

        Consumer<Object> availableCompat = (items) -> {
            var actualItems = (items instanceof String) ? List.of(items) : items;
            assertThat(actualItems)
                .asInstanceOf(InstanceOfAssertFactories.list(String.class))
                .isSubsetOf(expectedCompatibilities)
                .doesNotContainAnyElementsOf(allCompatibilities);
        };

        Consumer<Object> unavailableCompat = (items) -> {
            var actualItems = (items instanceof String) ? List.of(items) : items;
            assertThat(actualItems)
                .asInstanceOf(InstanceOfAssertFactories.list(String.class))
                .isSubsetOf(allCompatibilities)
                .doesNotContainAnyElementsOf(expectedCompatibilities);
        };

        assertThat(result)
            .hasSizeGreaterThanOrEqualTo(45)
            .containsEntry("gdsVersion", buildInfoProperties.gdsVersion())
            .containsEntry("minimumRequiredJavaVersion", buildInfoProperties.minimumRequiredJavaVersion())
            .containsEntry("buildDate", buildInfoProperties.buildDate())
            .containsEntry("buildJdk", buildInfoProperties.buildJdk())
            .containsEntry("buildJavaVersion", buildInfoProperties.buildJavaVersion())
            .containsEntry("buildHash", buildInfoProperties.buildHash())
            .containsEntry("neo4jVersion", Version.getNeo4jVersion())
            .hasEntrySatisfying("availableCompatibility", availableCompat)
            .hasEntrySatisfying("unavailableCompatibility", unavailableCompat)
            .hasEntrySatisfying("gdsEdition", GDS_EDITION)
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
            .containsEntry(Settings.pageCacheMemory().name(), Settings.pageCacheMemoryValue("42M"))
            .containsEntry(Settings.transactionStateAllocation().name(), "ON_HEAP")
            .containsEntry(Settings.transactionStateMaxOffHeapMemory().name(), 1337L);
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
