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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskTreeProgressTracker;
import org.neo4j.gds.logging.Log;

/**
 * Just some convenience, address this one day when we attack ProgressTracker
 */
public class ProgressTrackerCreator {
    private final Log log;
    private final RequestScopedDependencies requestScopedDependencies;

    public ProgressTrackerCreator(Log log, RequestScopedDependencies requestScopedDependencies) {
        this.log = log;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    public ProgressTracker createProgressTracker(AlgoBaseConfig configuration, Task task) {
        if (configuration.logProgress()) {
            return new TaskProgressTracker(
                task,
                (org.neo4j.logging.Log) log.getNeo4jLog(),
                configuration.typedConcurrency(),
                configuration.jobId(),
                requestScopedDependencies.getTaskRegistryFactory(),
                requestScopedDependencies.getUserLogRegistryFactory()
            );
        }

        return new TaskTreeProgressTracker(
            task,
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            configuration.typedConcurrency(),
            configuration.jobId(),
            requestScopedDependencies.getTaskRegistryFactory(),
            requestScopedDependencies.getUserLogRegistryFactory()
        );
    }
}
