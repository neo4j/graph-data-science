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
package org.neo4j.gds.procedures.integration;

import org.neo4j.gds.core.utils.progress.EmptyTaskStore;
import org.neo4j.gds.core.utils.progress.PerDatabaseTaskStore;
import org.neo4j.gds.core.utils.progress.TaskStore;

import java.time.Duration;

public class TaskStoreFactory {
    private final Duration retentionPeriod;

    private TaskStoreFactory(Duration retentionPeriod) {
        this.retentionPeriod = retentionPeriod;
    }

    /**
     * @param progressTrackingEnabled if not enabled you get shunt behaviour
     */
    static TaskStoreFactory create(boolean progressTrackingEnabled, Duration retentionPeriod) {
        if (!progressTrackingEnabled) return new TaskStoreFactory(null) {
            @Override
            public TaskStore create() {
                return EmptyTaskStore.INSTANCE;
            }
        };

        return new TaskStoreFactory(retentionPeriod);
    }

    TaskStore create() {
        return new PerDatabaseTaskStore(retentionPeriod);
    }
}
