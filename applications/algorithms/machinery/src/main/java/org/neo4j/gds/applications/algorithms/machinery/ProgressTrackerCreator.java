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
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.logging.LoggerForProgressTracking;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;

/**
 * Just some convenience on top of the more basic factory.
 */
public final class ProgressTrackerCreator {
    private final ProgressTrackerFactory delegate;

    private ProgressTrackerCreator(ProgressTrackerFactory delegate) {
        this.delegate = delegate;
    }

    public static ProgressTrackerCreator construct(
        Log log,
        LoggerForProgressTracking loggerForProgressTracking,
        RequestCorrelationId correlationId,
        TaskRegistryFactory taskRegistryFactory
    ) {
        var factory = new ProgressTrackerFactory(log, loggerForProgressTracking, correlationId, taskRegistryFactory);

        return new ProgressTrackerCreator(factory);
    }

    /**
     * Convenience, using domain concepts
     */
    public ProgressTracker create(Task task, AlgoBaseConfig configuration) {
        return delegate.create(task, configuration.jobId(), configuration.concurrency(), configuration.logProgress());
    }
}
