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
package org.neo4j.gds.system;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.core.utils.mem.MemoryUsage;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class SystemStatusProc extends BaseProc {

    private static final String DESCRIPTION = "Gives an overview of the system's resources and how they are currently being used.";

    @Procedure(name = "gds.alpha.systemStatus", mode = READ)
    @Description(DESCRIPTION)
    public Stream<SystemStatusResult> systemStatus() {

        SystemStatusResult result = runWithExceptionLogging(
            "Failed to collect system status information",
            SystemStatusResult::new
        );

        return Stream.of(result);
    }

    public static class SystemStatusResult {
        public final long jvmFreeMemory;
        public final long jvmTotalMemory;
        public final long jvmMaxMemory;
        public final long jvmAvailableProcessors;
        public final Map<String, String> description;

        public SystemStatusResult() {
            this.jvmFreeMemory = Runtime.getRuntime().freeMemory();
            this.jvmTotalMemory = Runtime.getRuntime().totalMemory();
            this.jvmMaxMemory = Runtime.getRuntime().maxMemory();
            this.jvmAvailableProcessors = Runtime.getRuntime().availableProcessors();
            this.description = Map.of(
                "jvmFreeMemory",
                MemoryUsage.humanReadable(this.jvmFreeMemory),
                "jvmTotalMemory",
                MemoryUsage.humanReadable(this.jvmTotalMemory),
                "jvmMaxMemory",
                MemoryUsage.humanReadable(this.jvmMaxMemory),
                "jvmAvailableProcessors",
                String.valueOf(this.jvmAvailableProcessors)
            );
        }
    }
}
