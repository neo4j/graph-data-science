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
        features().forEach(values::add);
        buildInfo(buildInfo).forEach(values::add);
        values.add(value("availableCPUs", runtime.availableProcessors()));
        memoryInfo(runtime).forEach(values::add);
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

    private static List<DebugValue> features() {
        return List.of(
            value("pre-aggregation", GdsFeatureToggles.USE_PRE_AGGREGATION.get()),
            value("skip-orphan-nodes", GdsFeatureToggles.SKIP_ORPHANS.get()),
            value("max-array-length-shift", (long) GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get()),
            value("kernel-tracker", GdsFeatureToggles.USE_KERNEL_TRACKER.get())
        );
    }

    private static List<DebugValue> buildInfo(BuildInfoProperties properties) {
        return List.of(
            value("buildDate", properties.buildDate()),
            value("buildJdk", properties.buildJdk()),
            value("buildJavaVersion", properties.buildJavaVersion()),
            value("buildHash", properties.buildHash())
        );
    }

    private static List<DebugValue> memoryInfo(Runtime runtime) {
        var totalHeapInBytes = runtime.totalMemory();
        var maxHeapInBytes = runtime.maxMemory();
        var freeHeapInBytes = runtime.freeMemory();
        var availableHeapInBytes = GcListenerExtension.freeMemory();
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        return List.of(
            value("totalHeapInBytes", totalHeapInBytes),
            value("totalHeap", humanReadable(totalHeapInBytes)),
            value("maxHeapInBytes", maxHeapInBytes),
            value("maxHeap", humanReadable(maxHeapInBytes)),
            value("freeHeapInBytes", freeHeapInBytes),
            value("freeHeap", humanReadable(freeHeapInBytes)),
            value("availableHeapInBytes", availableHeapInBytes),
            value("availableHeap", humanReadable(availableHeapInBytes)),

            value("heapUsage", memBean.getHeapMemoryUsage().toString()),
            value("nonHeapUsage", memBean.getNonHeapMemoryUsage().toString())
        );
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
