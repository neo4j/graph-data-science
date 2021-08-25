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

import org.neo4j.gds.core.utils.mem.MemoryUsage;
import org.neo4j.gds.core.utils.progress.ProgressEventStore;
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
    public ProgressEventStore progress;

    private static final String DESCRIPTION = "Gives an overview of the system's resources and how they are currently being used.";

    @Procedure(name = "gds.alpha.systemMonitor", mode = READ)
    @Description(DESCRIPTION)
    public Stream<SystemMonitorResult> systemMonitor() {

        SystemMonitorResult result = runWithExceptionLogging(
            "Failed to collect system status information",
            SystemMonitorResult::new
        );

        return Stream.of(result);
    }

    public class SystemMonitorResult {

        public final long jvmFreeMemory;
        public final long jvmTotalMemory;
        public final long jvmMaxMemory;
        public final long jvmAvailableProcessors;
        public final Map<String, String> jvmStatusDescription;
        public final List<Map<String, String>> ongoingGdsProcedures;

        public SystemMonitorResult() {
            var runtime = Runtime.getRuntime();
            this.jvmFreeMemory = runtime.freeMemory();
            this.jvmTotalMemory = runtime.totalMemory();
            this.jvmMaxMemory = runtime.maxMemory();
            this.jvmAvailableProcessors = runtime.availableProcessors();

            this.jvmStatusDescription = Map.of(
                "jvmFreeMemory",
                MemoryUsage.humanReadable(this.jvmFreeMemory),
                "jvmTotalMemory",
                MemoryUsage.humanReadable(this.jvmTotalMemory),
                "jvmMaxMemory",
                MemoryUsage.humanReadable(this.jvmMaxMemory),
                "jvmAvailableProcessors",
                String.valueOf(this.jvmAvailableProcessors)
            );

            this.ongoingGdsProcedures = getAllOngoingProcedures();
        }

        private List<Map<String, String>> getAllOngoingProcedures() {
            return progress
                .query()
                .stream()
                .map(event ->
                {
                    var progress = event.task().getProgress();
                    return Map.of(
                        "taskName",
                        event.task().description(),
                        "progress",
                        StructuredOutputHelper.computeProgress(
                            progress.progress(),
                            progress.volume()
                        )
                    );
                })
                .collect(toList());
        }
    }
}
