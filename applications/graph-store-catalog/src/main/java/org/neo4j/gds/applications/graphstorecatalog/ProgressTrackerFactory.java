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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.LoggerForProgressTracking;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;

class ProgressTrackerFactory {
    private final LoggerForProgressTracking log;
    private final RequestCorrelationId requestCorrelationId;
    private final TaskRegistryFactory taskRegistryFactory;

    ProgressTrackerFactory(
        LoggerForProgressTracking log,
        RequestCorrelationId requestCorrelationId,
        TaskRegistryFactory taskRegistryFactory
    ) {
        this.log = log;
        this.requestCorrelationId = requestCorrelationId;
        this.taskRegistryFactory = taskRegistryFactory;
    }

    ProgressTracker create(Task task) {
        var jobId = new JobId();
        var concurrency = new Concurrency(1);

        return TaskProgressTracker.create(
            log,
            task,
            concurrency,
            jobId,
            requestCorrelationId,
            taskRegistryFactory
        );
    }
}
