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

import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.utils.mem.GcListenerExtension;
import org.neo4j.graphalgo.utils.GdsFeatureToggles;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.kernel.diagnostics.providers.SystemDiagnostics;
import org.neo4j.kernel.internal.Version;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.DebugProc.DebugValue.value;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;

public class DebugProc {

    @Procedure("gds.debug")
    public Stream<DebugValue> version() throws IOException {
        var properties = BuildInfoProperties.get();
        return debugValues(properties, Runtime.getRuntime(), GdsEdition.instance());
    }

    public static final class DebugValue {
        public final String key;
        public final Object value;

        private DebugValue(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public static DebugValue value(String key, Object value) {
            return new DebugValue(key, value);
        }
    }

    private static Stream<DebugValue> debugValues(
        BuildInfoProperties buildInfo,
        Runtime runtime,
        GdsEdition gdsEdition
    ) {
        var values = Stream.<DebugValue>builder();
        values.add(value("gdsVersion", buildInfo.gdsVersion()));
        values.add(value("gdsEdition", editionString(gdsEdition)));
        values.add(value("neo4jVersion", Version.getNeo4jVersion()));
        values.add(value("minimumRequiredJavaVersion", buildInfo.minimumRequiredJavaVersion()));
        features(values);
        buildInfo(buildInfo, values);
        values.add(value("availableCPUs", runtime.availableProcessors()));
        memoryInfo(values);
        systemDiagnostics().forEach(values::add);
        return values.build();
    }

    private static String editionString(GdsEdition edition) {
        if (edition.isInvalidLicense()) {
            return "Enterprise (invalid license)";
        }
        if (edition.isOnEnterpriseEdition()) {
            return "Enterprise";
        }
        if (edition.isOnCommunityEdition()) {
            return "Community";
        }
        return "Unknown";
    }

    private static void features(Stream.Builder<DebugValue> builder) {
        builder
            .add(value("pre-aggregation", GdsFeatureToggles.USE_PRE_AGGREGATION.get()))
            .add(value("skip-orphan-nodes", GdsFeatureToggles.SKIP_ORPHANS.get()))
            .add(value("max-array-length-shift", (long) GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get()))
            .add(value("kernel-tracker", GdsFeatureToggles.USE_KERNEL_TRACKER.get()));
    }

    private static void buildInfo(BuildInfoProperties properties, Stream.Builder<DebugValue> builder) {
        builder
            .add(value("buildDate", properties.buildDate()))
            .add(value("buildJdk", properties.buildJdk()))
            .add(value("buildJavaVersion", properties.buildJavaVersion()))
            .add(value("buildHash", properties.buildHash()));
    }

    private static void memoryInfo(Stream.Builder<DebugValue> builder) {
        var availableHeapInBytes = GcListenerExtension.freeMemory();
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        builder
            .add(value("availableHeapInBytes", availableHeapInBytes))
            .add(value("availableHeap", humanReadable(availableHeapInBytes)));
        onHeapInfo(memBean.getHeapMemoryUsage(), builder);
        offHeapInfo(memBean.getNonHeapMemoryUsage(), builder);
    }

    private static void onHeapInfo(MemoryUsage memUsage, Stream.Builder<DebugValue> builder) {
        var maxHeapInBytes = memUsage.getMax();
        var totalHeapInBytes = memUsage.getCommitted();
        var freeHeapInBytes = memUsage.getCommitted() - memUsage.getUsed();

        builder
            .add(value("freeHeapInBytes", freeHeapInBytes))
            .add(value("freeHeap", humanReadable(freeHeapInBytes)))
            .add(value("totalHeapInBytes", totalHeapInBytes))
            .add(value("totalHeap", humanReadable(totalHeapInBytes)))
            .add(value("maxHeapInBytes", maxHeapInBytes))
            .add(value("maxHeap", humanReadable(maxHeapInBytes)));
    }

    private static void offHeapInfo(MemoryUsage memUsage, Stream.Builder<DebugValue> builder) {
        var totalOffHeapInBytes = memUsage.getCommitted();
        var usedOffHeapInBytes = memUsage.getUsed();
        builder
            .add(value("usedOffHeapInBytes", usedOffHeapInBytes))
            .add(value("usedOffHeap", humanReadable(usedOffHeapInBytes)))
            .add(value("totalOffHeapInBytes", totalOffHeapInBytes))
            .add(value("totalOffHeap", humanReadable(totalOffHeapInBytes)));
    }

    // classpath for duplicate entries or other plugins
    private static List<DebugValue> systemDiagnostics() {
        var values = new ArrayList<DebugValue>();
        var index = new AtomicInteger();
        DiagnosticsLogger collectDiagnostics = line -> values.add(value("sp" + index.getAndIncrement(), line));
        Stream.of(
            SystemDiagnostics.SYSTEM_MEMORY,
            SystemDiagnostics.JAVA_MEMORY,
            SystemDiagnostics.OPERATING_SYSTEM,
            SystemDiagnostics.JAVA_VIRTUAL_MACHINE,
            SystemDiagnostics.CONTAINER
        ).forEach(diagnostics -> diagnostics.dump(collectDiagnostics));
        return values;
    }
}
