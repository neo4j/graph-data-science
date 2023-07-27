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
import org.neo4j.gds.config.GraphDropNodePropertiesConfig;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;

import java.util.Optional;

public class DropNodePropertiesService {
    private final Log log;

    public DropNodePropertiesService(Log log) {
        this.log = log;
    }

    public long compute(
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphDropNodePropertiesConfig configuration,
        GraphStore graphStore,
        Optional<String> deprecationWarning
    ) {
        return computeWithProgressTracking(
            taskRegistryFactory,
            userLogRegistryFactory,
            configuration,
            graphStore,
            deprecationWarning
        );
    }

    private long computeWithProgressTracking(
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphDropNodePropertiesConfig configuration,
        GraphStore graphStore,
        Optional<String> deprecationWarning
    ) {
        var task = Tasks.leaf("Graph :: NodeProperties :: Drop", configuration.nodeProperties().size());
        var progressTracker = new TaskProgressTracker(
            task,
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            1,
            new JobId(),
            taskRegistryFactory,
            userLogRegistryFactory
        );

        deprecationWarning.ifPresent(progressTracker::logWarning);

        return computeWithErrorHandling(graphStore, configuration, progressTracker);
    }

    private long computeWithErrorHandling(
        GraphStore graphStore,
        GraphDropNodePropertiesConfig configuration,
        ProgressTracker progressTracker
    ) {
        try {
            return dropNodeProperties(graphStore, configuration, progressTracker);
        } catch (RuntimeException e) {
            log.warn("Node property removal failed", e);
            throw e;
        }
    }

    private Long dropNodeProperties(
        GraphStore graphStore,
        GraphDropNodePropertiesConfig configuration,
        ProgressTracker progressTracker
    ) {
        var removedPropertiesCount = new MutableLong(0);

        progressTracker.beginSubTask();
        configuration.nodeProperties().forEach(propertyKey -> {
            removedPropertiesCount.add(graphStore.nodeProperty(propertyKey).values().nodeCount());
            graphStore.removeNodeProperty(propertyKey);
            progressTracker.logProgress();
        });

        progressTracker.endSubTask();
        return removedPropertiesCount.longValue();
    }
}
