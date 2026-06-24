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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.progress.logging.LoggerForProgressTracking;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tasks.Tasks;
import org.neo4j.gds.logging.Log;

import java.util.List;

public class DropNodePropertiesApplication {
    private final Log log;
    private final LoggerForProgressTracking loggerForProgressTracking;

    DropNodePropertiesApplication(Log log, LoggerForProgressTracking loggerForProgressTracking) {
        this.log = log;
        this.loggerForProgressTracking = loggerForProgressTracking;
    }

    public long compute(
        RequestScopedDependencies requestScopedDependencies,
        Concurrency concurrency,
        List<String> nodeProperties,
        GraphStore graphStore,
        JobId jobId
    ) {
        var progressTrackerFactory = new ProgressTrackerFactory(
            log,
            loggerForProgressTracking,
            requestScopedDependencies.correlationId(),
            requestScopedDependencies.taskRegistryFactory()
        );

        return computeWithProgressTracking(
            graphStore,
            progressTrackerFactory,
            concurrency,
            nodeProperties,
            jobId
        );
    }

    private long computeWithProgressTracking(
        GraphStore graphStore,
        ProgressTrackerFactory progressTrackerFactory,
        Concurrency concurrency,
        List<String> nodeProperties,
        JobId jobId
    ) {
        var task = Tasks.leaf("Graph :: NodeProperties :: Drop", concurrency, nodeProperties.size());

        var progressTracker = progressTrackerFactory.create(task, jobId, new Concurrency(1), true);

        return computeWithErrorHandling(graphStore, progressTracker, nodeProperties);
    }

    private long computeWithErrorHandling(
        GraphStore graphStore,
        ProgressTracker progressTracker,
        List<String> nodeProperties
    ) {
        try {
            return dropNodeProperties(graphStore, progressTracker, nodeProperties);
        } catch (RuntimeException e) {
            loggerForProgressTracking.warn("Node property removal failed", e);
            throw e;
        }
    }

    private Long dropNodeProperties(
        GraphStore graphStore,
        ProgressTracker progressTracker,
        List<String> nodeProperties
    ) {
        var removedPropertiesCount = new MutableLong(0);

        progressTracker.beginSubTask();
        nodeProperties.forEach(propertyKey -> {
            removedPropertiesCount.add(graphStore.nodeProperty(propertyKey).values().nodeCount());
            graphStore.removeNodeProperty(propertyKey);
            progressTracker.onProgress();
        });

        progressTracker.endSubTask();
        return removedPropertiesCount.longValue();
    }
}
