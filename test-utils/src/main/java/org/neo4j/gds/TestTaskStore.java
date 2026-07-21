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
package org.neo4j.gds;

import org.neo4j.gds.api.User;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.progress.registration.PerDatabaseTaskStore;
import org.neo4j.gds.progress.registration.StoredTask;
import org.neo4j.gds.progress.tasks.Task;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TestTaskStore extends PerDatabaseTaskStore {
    private final List<String> tasksSeen = new ArrayList<>();

    public TestTaskStore() {
        this(Duration.ofMinutes(5));
    }

    public TestTaskStore(Duration retentionPeriod) {
        super(retentionPeriod);
    }

    @Override
    protected StoredTask storeTask(User user, JobId jobId, Task task) {
        tasksSeen.add(task.description());

        return super.storeTask(user, jobId, task);
    }

    @Override
    protected Optional<StoredTask> removeTask(User user, JobId jobId) {
        return super.removeTask(user, jobId);
    }

    public List<String> tasksSeen() {
        return tasksSeen;
    }
}
