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
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.DebugProc.DebugValue.value;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;

public class DebugProc {

    @Procedure("gds.debug")
    public Stream<DebugValue> version() throws IOException {
        var properties = LoadInfoProperties.infoProperties();
        var debugInfo = new DebugInfo(properties);

        return Stream.of(
            value("gdsVersion", debugInfo.gdsVersion),
            value("gdsEdition", debugInfo.gdsEdition),
            value("neo4jVersion", debugInfo.neo4jVersion),
            value("features", debugInfo.features),
            value("buildInfo", debugInfo.buildInfo),
            value("availableCPUs", debugInfo.availableCPUs),
            value("memoryInfo", debugInfo.memoryInfo),
            value("systemDiagnostics", debugInfo.systemDiagnostics)
        );
    }

    public static final class DebugValue {
        public final String key;
        public final Object value;

        private DebugValue(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public static DebugValue of(String key, Object value) {
            return new DebugValue(key, value);
        }

        public static DebugValue value(String key, Object value) {
            return new DebugValue(key, value);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static final class DebugInfo {
        public final String gdsVersion;
        public final String gdsEdition;
        public final String neo4jVersion;
        public final MapValue features;
        public final MapValue buildInfo;
        public final long availableCPUs;
        public final MapValue memoryInfo;
        public final ListValue systemDiagnostics;

        DebugInfo(Properties properties) {
            this.gdsVersion = properties.getProperty("Implementation-Version");
            this.gdsEdition = editionString(GdsEdition.instance());
            this.neo4jVersion = Version.getNeo4jVersion();
            this.features = features();
            this.buildInfo = buildInfo(properties);
            var runtime = Runtime.getRuntime();
            this.availableCPUs = runtime.availableProcessors();
            this.memoryInfo = memoryInfo(runtime);
            this.systemDiagnostics = systemDiagnostics();
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

        private static MapValue features() {
            var builder = new MapValueBuilder(4);
            builder.add("pre-aggregation", Values.booleanValue(GdsFeatureToggles.USE_PRE_AGGREGATION.get()));
            builder.add("skip-orphan-nodes", Values.booleanValue(GdsFeatureToggles.SKIP_ORPHANS.get()));
            builder.add("max-array-length-shift", Values.intValue(GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get()));
            builder.add("kernel-tracker", Values.booleanValue(GdsFeatureToggles.USE_KERNEL_TRACKER.get()));
            return builder.build();
        }

        private static MapValue buildInfo(Properties properties) {
            var builder = new MapValueBuilder(4);
            builder.add("buildDate", Values.stringValue(properties.getProperty("Build-Date")));
            builder.add("buildJdk", Values.stringValue(properties.getProperty("Created-By")));
            builder.add("buildJavaVersion", Values.stringValue(properties.getProperty("Build-Java-Version")));
            builder.add("buildHash", Values.stringValue(properties.getProperty("Full-Change")));
            return builder.build();
        }

        private static MapValue memoryInfo(Runtime runtime) {
            var totalHeapInBytes = runtime.totalMemory();
            var maxHeapInBytes = runtime.maxMemory();
            var freeHeapInBytes = runtime.freeMemory();
            var availableHeapInBytes = GcListenerExtension.freeMemory();
            var builder = new MapValueBuilder(10);
            builder.add("totalHeapInBytes", Values.longValue(totalHeapInBytes));
            builder.add("totalHeap", Values.stringValue(humanReadable(totalHeapInBytes)));
            builder.add("maxHeapInBytes", Values.longValue(maxHeapInBytes));
            builder.add("maxHeap", Values.stringValue(humanReadable(maxHeapInBytes)));
            builder.add("freeHeapInBytes", Values.longValue(freeHeapInBytes));
            builder.add("freeHeap", Values.stringValue(humanReadable(freeHeapInBytes)));
            builder.add("availableHeapInBytes", Values.longValue(availableHeapInBytes));
            builder.add("availableHeap", Values.stringValue(humanReadable(availableHeapInBytes)));
            MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
            builder.add("heapUsage", Values.stringValue(memBean.getHeapMemoryUsage().toString()));
            builder.add("nonHeapUsage", Values.stringValue(memBean.getNonHeapMemoryUsage().toString()));
            return builder.build();
        }

        // classpath for duplicate entries or other plugins
        private static ListValue systemDiagnostics() {
            var builder = ListValueBuilder.newListBuilder();
            DiagnosticsLogger collectDiagnostics = line -> builder.add(Values.stringValue(line));
            Stream.of(
                SystemDiagnostics.SYSTEM_MEMORY,
                SystemDiagnostics.JAVA_MEMORY,
                SystemDiagnostics.OPERATING_SYSTEM,
                SystemDiagnostics.JAVA_VIRTUAL_MACHINE,
                SystemDiagnostics.CONTAINER
            ).forEach(diagnostics -> diagnostics.dump(collectDiagnostics));
            return builder.build();
        }
    }

    // nested static class so that we don't load the properties when the proc class
    // is initialized, but only on first request, and then we cache it
    private static final class LoadInfoProperties {

        static Properties infoProperties() throws IOException {
            var properties = INFO_PROPERTIES;
            if (properties instanceof Properties) {
                return (Properties) properties;
            }
            throw (IOException) properties;
        }

        private static final String INFO_FILE = "META-INF/info.properties";
        private static final Object INFO_PROPERTIES = loadProperties();

        private static Object loadProperties() {
            var properties = new Properties();
            var classLoader = Thread.currentThread().getContextClassLoader();
            try (var infoStream = classLoader.getResourceAsStream(INFO_FILE)) {
                if (infoStream != null) {
                    try (var infoReader = new InputStreamReader(infoStream, StandardCharsets.UTF_8)) {
                        properties.load(infoReader);
                    }
                }
            } catch (IOException exception) {
                return exception;
            }
            return properties;
        }
    }
}
