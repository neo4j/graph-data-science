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
package org.neo4j.gds.executor;

import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;

public final class AlgorithmSpecProgressTrackerProvider {

    private AlgorithmSpecProgressTrackerProvider() {}

    public static ProgressTracker createProgressTracker(
        String taskName,
        long taskVolume,
        int writeConcurrency,
        ExecutionContext executionContext
    ) {
        return new TaskProgressTracker(
            NodePropertyExporter.baseTask(taskName, taskVolume),
            executionContext.log(),
            writeConcurrency,
            executionContext.taskRegistryFactory()
        );
    }

}
