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
package org.neo4j.gds.core.utils.progress;

import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.util.List;
import java.util.Optional;

public abstract class ObservableTaskStore implements TaskStore {
    private final List<TaskStoreListener> listeners;

    ObservableTaskStore(List<TaskStoreListener> listeners) {this.listeners = listeners;}

    @Override
    public final void store(String username, JobId jobId, Task task) {
        var userTask = storeUserTask(username, jobId, task);
        listeners.forEach(listener -> listener.onTaskAdded(userTask));
    }

    @Override
    public final void remove(String username, JobId jobId) {
        var userTask = removeUserTask(username, jobId);
        userTask.ifPresent(task -> listeners.forEach(listener -> listener.onTaskRemoved(task)));
    }

    @Override
    public synchronized void addListener(TaskStoreListener listener) {
        this.listeners.add(listener);
    }

    protected abstract UserTask storeUserTask(String username, JobId jobId, Task task);

    protected abstract Optional<UserTask> removeUserTask(String username, JobId jobId);
}
