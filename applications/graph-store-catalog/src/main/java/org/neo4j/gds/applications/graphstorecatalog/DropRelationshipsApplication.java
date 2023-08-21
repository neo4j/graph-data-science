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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.DeletionResult;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;

import java.util.Optional;

public class DropRelationshipsApplication {
    private final Log log;

    public DropRelationshipsApplication(Log log) {
        this.log = log;
    }

    public DeletionResult compute(
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphStore graphStore,
        String relationshipType,
        Optional<String> deprecationWarning
    ) {
        var progressTrackerFactory = new ProgressTrackerFactory(
            log,
            taskRegistryFactory,
            userLogRegistryFactory
        );
        var task = Tasks.leaf("Graph :: Relationships :: Drop", 1);
        var progressTracker = progressTrackerFactory.create(task);

        return computeWithProgressTracking(graphStore, relationshipType, deprecationWarning, progressTracker);
    }

    static DeletionResult computeWithProgressTracking(
        GraphStore graphStore,
        String relationshipType,
        Optional<String> deprecationWarning,
        ProgressTracker progressTracker
    ) {
        deprecationWarning.ifPresent(progressTracker::logWarning);

        progressTracker.beginSubTask();
        var deletionResult = graphStore.deleteRelationships(RelationshipType.of(relationshipType));
        progressTracker.endSubTask();

        return deletionResult;
    }
}
