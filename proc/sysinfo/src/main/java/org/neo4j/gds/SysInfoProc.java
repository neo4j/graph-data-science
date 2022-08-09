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

import org.apache.commons.text.WordUtils;
import org.neo4j.configuration.Config;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.Settings;
import org.neo4j.gds.core.utils.mem.GcListenerExtension;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.internal.Version;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.neo4j.gds.SysInfoProc.DebugValue.value;
import static org.neo4j.gds.mem.MemoryUsage.humanReadable;

// don't extend BaseProc and only inject GraphDatabaseService so that
// we can run this procedure even if unrestricted=gds.* had not been configured
public class SysInfoProc {

    @Context
    public GraphDatabaseService db;

    @Context
    public LicenseState licenseState;

    @Procedure("gds.debug.sysInfo")
    @Description("Returns details about the status of the system")
    public Stream<DebugValue> version() throws IOException {
        var properties = BuildInfoProperties.get();
        var config = Optional.ofNullable(db).map(db -> GraphDatabaseApiProxy.resolveDependency(db, Config.class));
        return debugValues(properties, Runtime.getRuntime(), config);
    }

    public static final class DebugValue {
        public final String key;
        public final Object value;

        private DebugValue(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        static DebugValue value(String key, Object value) {
            return new DebugValue(key, value);
        }
    }

    private Stream<DebugValue> debugValues(BuildInfoProperties buildInfo, Runtime runtime, Optional<Config> config) {
        var values = Stream.<DebugValue>builder();
        values.add(value("gdsVersion", buildInfo.gdsVersion()));
        editionInfo(values);
        values.add(value("neo4jVersion", Version.getNeo4jVersion()));
        values.add(value("minimumRequiredJavaVersion", buildInfo.minimumRequiredJavaVersion()));
        features(values);
        buildInfo(buildInfo, values);
        cpuInfo(runtime, values);
        memoryInfo(values);
        systemResources(values);
        vmInfo(values);
        containerInfo(values);
        config.ifPresent(cfg -> configInfo(cfg, values));
        return values.build();
    }

    private void editionInfo(Stream.Builder<DebugValue> builder) {
        licenseState.visit(ADD_EDITION_INFO.INSTANCE, builder);
    }

    private enum ADD_EDITION_INFO implements LicenseState.VisitorWithParameter<Void, Stream.Builder<DebugValue>> {
        INSTANCE;

        @Override
        public Void unlicensed(String name, Stream.Builder<DebugValue> builder) {
            builder.add(value("gdsEdition", name));
            return null;
        }

        @Override
        public Void licensed(String name, ZonedDateTime expirationTime, Stream.Builder<DebugValue> builder) {
            unlicensed(name, builder);
            builder.add(value("gdsLicenseExpirationTime", expirationTime));
            return null;
        }

        @Override
        public Void invalid(
            String name,
            String errorMessage,
            Optional<ZonedDateTime> expirationTime,
            Stream.Builder<DebugValue> builder
        ) {
            expirationTime.ifPresentOrElse(
                expiration -> licensed(name, expiration, builder),
                () -> unlicensed(name, builder)
            );
            builder.add(value("gdsLicenseError", errorMessage));
            return null;
        }
    }

    private static void features(Stream.Builder<DebugValue> builder) {
        builder
            .add(value("featureSkipOrphanNodes", GdsFeatureToggles.SKIP_ORPHANS.isEnabled()))
            .add(value("featureMaxArrayLengthShift", GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get()))
            .add(value("featurePropertyValueIndex", GdsFeatureToggles.USE_PROPERTY_VALUE_INDEX.isEnabled()))
            .add(value(
                "featureParallelPropertyValueIndex",
                GdsFeatureToggles.USE_PARALLEL_PROPERTY_VALUE_INDEX.isEnabled()
            ))
            .add(value("featurePartitionedScan", GdsFeatureToggles.USE_PARTITIONED_SCAN.isEnabled()))
            .add(value("featureBitIdMap", GdsFeatureToggles.USE_BIT_ID_MAP.isEnabled()))
            .add(value("featureShardedIdMap", GdsFeatureToggles.USE_SHARDED_ID_MAP.isEnabled()))
            .add(value(
                "featureUncompressedAdjacencyList",
                GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
            ))
            .add(value("featureReorderedAdjacencyList", GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.isEnabled()));

    }

    private static void buildInfo(BuildInfoProperties properties, Stream.Builder<DebugValue> builder) {
        builder
            .add(value("buildDate", properties.buildDate()))
            .add(value("buildJdk", properties.buildJdk()))
            .add(value("buildJavaVersion", properties.buildJavaVersion()))
            .add(value("buildHash", properties.buildHash()));
    }

    private static void cpuInfo(Runtime runtime, Stream.Builder<DebugValue> values) {
        values.add(value("availableCPUs", runtime.availableProcessors()));
        values.add(value("physicalCPUs", ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors()));
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

    private static void vmInfo(Stream.Builder<DebugValue> builder) {
        var runtime = ManagementFactory.getRuntimeMXBean();
        var compiler = ManagementFactory.getCompilationMXBean();

        builder
            .add(value("vmName", runtime.getVmName()))
            .add(value("vmVersion", runtime.getVmVersion()))
            .add(value("vmCompiler", compiler == null ? "Unknown" : compiler.getName()));
    }

    private static void containerInfo(Stream.Builder<DebugValue> builder) {
        boolean containerized = false;

        // test for Docker
        try (Stream<String> stream = Files.lines(Paths.get("/proc/1/cgroup"))) {
            if (stream.anyMatch(line -> line.contains("/docker"))) {
                containerized = true;
            }
        } catch (IOException ignored) {
        }

        // test for LXC
        if (!containerized) {
            containerized = "lxc".equals(System.getProperty("container"));
        }

        // test for Kubernetes
        if (!containerized) {
            containerized = System.getProperty("KUBERNETES_SERVICE_HOST") != null;
        }

        builder.add(value("containerized", containerized));
    }

    private static void configInfo(Config config, Consumer<DebugValue> builder) {
        builder.accept(configVal(config, Settings.procedureUnrestricted(), s -> String.join(",", s)));
        builder.accept(configVal(config, Settings.transactionStateAllocation(), Enum::name));

        // the following keys are different on different Neo4j versions, we add those that are available

        trySetting("dbms.memory.pagecache.size", config, builder);
        trySetting("server.memory.pagecache.size", config, builder);

        trySetting("dbms.tx_state.max_off_heap_memory", config, builder);
        trySetting("dbms.memory.off_heap.max_size", config, builder);
        trySetting("server.memory.off_heap.max_size", config, builder);

        trySetting("dbms.memory.transaction.global_max_size", config, builder);
        trySetting("dbms.memory.transaction.total.max", config, builder);

        trySetting("dbms.memory.transaction.datababase_max_size", config, builder);
        trySetting("db.memory.transaction.total.max", config, builder);

        trySetting("dbms.memory.transaction.max_size", config, builder);
        trySetting("db.memory.transaction.max", config, builder);
    }

    private static DebugValue configVal(Configuration config, Setting<?> setting) {
        return value(setting.name(), config.get(setting));
    }

    private static <T, U> DebugValue configVal(Configuration config, Setting<T> setting, Function<T, U> convert) {
        return value(setting.name(), convert.apply(config.get(setting)));
    }

    private static String safeHumanReadable(long bytes) {
        if (bytes < 0) {
            return "N/A";
        }
        return humanReadable(bytes);
    }

    private static void trySetting(String name, Config config, Consumer<DebugValue> builder) {
        try {
            var setting = config.getSetting(name);
            builder.accept(configVal(config, setting));
        } catch (IllegalArgumentException ignored) {
        }
    }
}
