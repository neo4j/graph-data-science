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

import org.neo4j.gds.core.GdsEdition;
import org.neo4j.gds.core.utils.mem.MemoryUsage;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.neo4j.procedure.Mode.READ;

public class SystemMonitorProc extends BaseProc {

    @Context
    public TaskStore taskStore;

    private static final String DESCRIPTION = "Get an overview of the system's workload and available resources";

    @Procedure(name = "gds.alpha.systemMonitor", mode = READ)
    @Description(DESCRIPTION)
    public Stream<SystemMonitorResult> systemMonitor() {
        if (GdsEdition.instance().isOnCommunityEdition()) {
            throw new RuntimeException(
                "This feature is only available with the Graph Data Science library enterprise edition");
        }

        SystemMonitorResult result = runWithExceptionLogging(
            "Failed to collect system status information",
            SystemMonitorResult::new
        );

        return Stream.of(result);
    }

    public class SystemMonitorResult {

        public final long freeHeap;
        public final long totalHeap;
        public final long maxHeap;
        public final long jvmAvailableCpuCores;
        public final Map<String, String> jvmHeapStatus;
        public final List<Map<String, String>> ongoingGdsProcedures;

        public SystemMonitorResult() {
            var runtime = Runtime.getRuntime();
            this.freeHeap = runtime.freeMemory();
            this.totalHeap = runtime.totalMemory();
            this.maxHeap = runtime.maxMemory();
            this.jvmAvailableCpuCores = runtime.availableProcessors();

            this.jvmHeapStatus = Map.of(
                "freeHeap",
                MemoryUsage.humanReadable(this.freeHeap),
                "totalHeap",
                MemoryUsage.humanReadable(this.totalHeap),
                "maxHeap",
                MemoryUsage.humanReadable(this.maxHeap),
                "jvmAvailableCpuCores",
                String.valueOf(this.jvmAvailableCpuCores)
            );

            this.ongoingGdsProcedures = getAllOngoingProcedures();
        }

        private List<Map<String, String>> getAllOngoingProcedures() {
            return taskStore
                .taskStream()
                .map(task -> {
                    var progress = task.getProgress();
                    var estimatedMemoryRange = !task.estimatedMemoryRangeInBytes().isEmpty()
                        ? task.estimatedMemoryRangeInBytes().toString()
                        : "n/a";
                    var maxNumberOfCpuCores = task.maxConcurrency() != Task.UNKNOWN_CONCURRENCY
                        ? String.valueOf(task.maxConcurrency())
                        : "n/a";
                    return Map.of(
                        "procedure", task.description(),
                        "progress", StructuredOutputHelper.computeProgress(
                            progress.progress(),
                            progress.volume()
                        ),
                        "estimatedMemoryRange", estimatedMemoryRange,
                        "maxNumberOfCpuCores", maxNumberOfCpuCores
                    );
                })
                .collect(toList());
        }
    }
}
