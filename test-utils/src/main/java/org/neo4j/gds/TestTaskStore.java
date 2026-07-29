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
import org.neo4j.gds.progress.registration.TaskStore;
import org.neo4j.gds.progress.registration.TaskStoreListener;
import org.neo4j.gds.progress.tasks.Task;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class TestTaskStore implements TaskStore {
    private final List<String> tasksSeen = new ArrayList<>();

    private final TaskStore delegate = PerDatabaseTaskStore.create(Duration.ofMinutes(3));

    @Override
    public void store(User user, JobId jobId, Task task) {
        delegate.store(user, jobId, task);

        tasksSeen.add(task.description());
    }

    @Override
    public void remove(User user, JobId jobId) {
        delegate.remove(user, jobId);
    }

    @Override
    public void markCompleted(User user, JobId jobId) {
        delegate.markCompleted(user, jobId);
    }

    @Override
    public Stream<StoredTask> query() {
        return delegate.query();
    }

    @Override
    public Stream<StoredTask> query(JobId jobId) {
        return delegate.query(jobId);
    }

    @Override
    public Stream<StoredTask> query(User user) {
        return delegate.query(user);
    }

    @Override
    public Set<StoredTask> lookup(User user, JobId jobId) {
        return delegate.lookup(user, jobId);
    }

    @Override
    public long ongoingTaskCount() {
        return delegate.ongoingTaskCount();
    }

    @Override
    public void addListener(TaskStoreListener listener) {
        delegate.addListener(listener);
    }

    public List<String> tasksSeen() {
        return tasksSeen;
    }
}
