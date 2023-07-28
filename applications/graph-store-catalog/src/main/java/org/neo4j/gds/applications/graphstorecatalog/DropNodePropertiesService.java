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
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;

import java.util.List;
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
        var progressTrackerFactory = new ProgressTrackerFactory(
            log,
            taskRegistryFactory,
            userLogRegistryFactory
        );

        return computeWithProgressTracking(
            graphStore,
            deprecationWarning,
            progressTrackerFactory,
            configuration.nodeProperties()
        );
    }

    long computeWithProgressTracking(
        GraphStore graphStore,
        Optional<String> deprecationWarning,
        ProgressTrackerFactory progressTrackerFactory,
        List<String> nodeProperties
    ) {
        var task = Tasks.leaf("Graph :: NodeProperties :: Drop", nodeProperties.size());

        var progressTracker = progressTrackerFactory.create(task, 1);

        deprecationWarning.ifPresent(progressTracker::logWarning);

        return computeWithErrorHandling(graphStore, progressTracker, nodeProperties);
    }

    long computeWithErrorHandling(
        GraphStore graphStore,
        ProgressTracker progressTracker,
        List<String> nodeProperties
    ) {
        try {
            return dropNodeProperties(graphStore, progressTracker, nodeProperties);
        } catch (RuntimeException e) {
            log.warn("Node property removal failed", e);
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
            progressTracker.logProgress();
        });

        progressTracker.endSubTask();
        return removedPropertiesCount.longValue();
    }
}
