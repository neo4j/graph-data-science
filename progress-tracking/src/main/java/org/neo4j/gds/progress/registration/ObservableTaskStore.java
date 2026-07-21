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
package org.neo4j.gds.progress.registration;

import org.neo4j.gds.api.User;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.progress.tasks.Status;
import org.neo4j.gds.progress.tasks.Task;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public abstract class ObservableTaskStore implements TaskStore {
    private final Set<TaskStoreListener> listeners = new HashSet<>();

    @Override
    public void store(User user, JobId jobId, Task task) {
        var userTask = storeTask(user, jobId, task);
        listeners.forEach(listener -> listener.onTaskAdded(userTask));
    }

    @Override
    public void remove(User user, JobId jobId) {
        removeTask(user, jobId);
    }

    @Override
    public void markCompleted(User user, JobId jobId) {
        var storedTask = query(user, jobId);

        storedTask.map(StoredTask::task).ifPresent(task -> {
            if (task.status() == Status.PENDING) {
                task.cancel();
            } else if (task.status() == Status.RUNNING) {
                task.finish();
            }
        });
        storedTask.ifPresent(task -> listeners.forEach(listener -> listener.onTaskCompleted(task)));
    }

    @Override
    public synchronized void addListener(TaskStoreListener listener) {
        this.listeners.add(listener);
    }

    protected abstract StoredTask storeTask(User user, JobId jobId, Task task);

    protected abstract Optional<StoredTask> removeTask(User user, JobId jobId);
}
