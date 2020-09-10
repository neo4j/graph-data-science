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

import org.apache.commons.text.WordUtils;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.utils.mem.GcListenerExtension;
import org.neo4j.graphalgo.utils.GdsFeatureToggles;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.diagnostics.providers.SystemDiagnostics;
import org.neo4j.kernel.internal.Version;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
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
        values.add(value("physicalCPUs", ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors()));
        memoryInfo(values);
        systemResources(values);
        systemDiagnostics(values);
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
            .add(value("availableHeap", safeHumanReadable(availableHeapInBytes)));
        onHeapInfo("heap", memBean.getHeapMemoryUsage(), builder);
        offHeapInfo("offHeap", memBean.getNonHeapMemoryUsage(), builder);

        for (var pool : ManagementFactory.getMemoryPoolMXBeans()) {
            var usage = pool.getUsage();
            if (usage == null) {
                continue;
            }
            var name = "pool" + WordUtils
                .capitalizeFully(pool.getName(), ' ', '\'', '-')
                .replaceAll("[ '-]", "");
            switch (pool.getType()) {
                case HEAP:
                    onHeapInfo(name, usage, builder);
                    break;
                case NON_HEAP:
                    offHeapInfo(name, usage, builder);
                    break;
                default:
                    // do nothing
            }
        }
    }

    private static void onHeapInfo(String name, MemoryUsage memUsage, Stream.Builder<DebugValue> builder) {
        var maxHeapInBytes = memUsage.getMax();
        var totalHeapInBytes = memUsage.getCommitted();
        var freeHeapInBytes = memUsage.getCommitted() - memUsage.getUsed();

        builder
            .add(value(name + "FreeInBytes", freeHeapInBytes))
            .add(value(name + "Free", safeHumanReadable(freeHeapInBytes)))
            .add(value(name + "TotalInBytes", totalHeapInBytes))
            .add(value(name + "Total", safeHumanReadable(totalHeapInBytes)))
            .add(value(name + "MaxInBytes", maxHeapInBytes))
            .add(value(name + "Max", safeHumanReadable(maxHeapInBytes)));
    }

    private static void offHeapInfo(String name, MemoryUsage memUsage, Stream.Builder<DebugValue> builder) {
        var totalOffHeapInBytes = memUsage.getCommitted();
        var usedOffHeapInBytes = memUsage.getUsed();
        builder
            .add(value(name + "UsedInBytes", usedOffHeapInBytes))
            .add(value(name + "Used", safeHumanReadable(usedOffHeapInBytes)))
            .add(value(name + "TotalInBytes", totalOffHeapInBytes))
            .add(value(name + "Total", safeHumanReadable(totalOffHeapInBytes)));
    }

    private static void systemResources(Stream.Builder<DebugValue> builder) {
        var freePhysicalMemory = OsBeanUtil.getFreePhysicalMemory();
        var committedVirtualMemory = OsBeanUtil.getCommittedVirtualMemory();
        var totalPhysicalMemory = OsBeanUtil.getTotalPhysicalMemory();
        var freeSwapSpace = OsBeanUtil.getFreeSwapSpace();
        var totalSwapSpace = OsBeanUtil.getTotalSwapSpace();

        builder.add(value("freePhysicalMemoryInBytes", freePhysicalMemory));
        builder.add(value("freePhysicalMemory", safeHumanReadable(freePhysicalMemory)));
        builder.add(value("committedVirtualMemoryInBytes", committedVirtualMemory));
        builder.add(value("committedVirtualMemory", safeHumanReadable(committedVirtualMemory)));
        builder.add(value("totalPhysicalMemoryInBytes", totalPhysicalMemory));
        builder.add(value("totalPhysicalMemory", safeHumanReadable(totalPhysicalMemory)));
        builder.add(value("freeSwapSpaceInBytes", freeSwapSpace));
        builder.add(value("freeSwapSpace", safeHumanReadable(freeSwapSpace)));
        builder.add(value("totalSwapSpaceInBytes", totalSwapSpace));
        builder.add(value("totalSwapSpace", safeHumanReadable(totalSwapSpace)));
        builder.add(value("openFileDescriptors", OsBeanUtil.getOpenFileDescriptors()));
        builder.add(value("maxFileDescriptors", OsBeanUtil.getMaxFileDescriptors()));
    }

    // classpath for duplicate entries or other plugins
    private static void systemDiagnostics(Stream.Builder<DebugValue> builder) {
        var index = new AtomicInteger();
        DiagnosticsLogger collectDiagnostics = line -> builder.add(value("sp" + index.getAndIncrement(), line));
        Stream.of(
            SystemDiagnostics.JAVA_VIRTUAL_MACHINE,
            SystemDiagnostics.CONTAINER
        ).forEach(diagnostics -> diagnostics.dump(collectDiagnostics));
    }

    private static String safeHumanReadable(long bytes) {
        if (bytes < 0) {
            return "N/A";
        }
        return humanReadable(bytes);
    }
}
