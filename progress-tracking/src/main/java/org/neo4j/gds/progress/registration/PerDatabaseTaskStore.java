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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class PerDatabaseTaskStore implements TaskStore {
    private final Map<User, Map<JobId, StoredTask>> registeredTasks = new ConcurrentHashMap<>();
    private final Set<TaskStoreListener> listeners = ConcurrentHashMap.newKeySet();

    protected PerDatabaseTaskStore() {

    }

    public static PerDatabaseTaskStore create(Duration retentionPeriod) {
        var taskStore = new PerDatabaseTaskStore();

        var taskStoreCleaner = new TaskStoreCleaner(taskStore, retentionPeriod);
        taskStore.addListener(taskStoreCleaner);

        return taskStore;
    }

    @Override
    public void store(User user, JobId jobId, Task task) {
        var storedTask = new StoredTask(user, jobId, task);

        registeredTasks
            .computeIfAbsent(user, __ -> new ConcurrentHashMap<>())
            .put(jobId, storedTask);

        listeners.forEach(listener -> listener.onTaskAdded(storedTask));
    }

    @Override
    public void remove(User user, JobId jobId) {
        var tasksForUser = this.registeredTasks.get(user);

        if (tasksForUser == null) return;

        tasksForUser.remove(jobId);
    }

    @Override
    public void markCompleted(User user, JobId jobId) {
        var possibleStoredTask = lookup(user, jobId);

        if (possibleStoredTask.isEmpty()) return;

        var storedTask = possibleStoredTask.get();

        var task = storedTask.task();

        if (task.status() == Status.PENDING) {
            task.cancel();
        } else if (task.status() == Status.RUNNING) {
            task.finish();
        }

        listeners.forEach(listener -> listener.onTaskCompleted(storedTask));
    }

    @Override
    public Stream<StoredTask> query() {
        return registeredTasks
            .entrySet()
            .stream()
            .flatMap(tasksPerUsers -> tasksPerUsers
                .getValue()
                .values()
                .stream());
    }

    @Override
    public Stream<StoredTask> query(JobId jobId) {
        return query().filter(storedTask -> storedTask.jobId().equals(jobId));
    }

    @Override
    public Stream<StoredTask> query(User user) {
        return registeredTasks
            .getOrDefault(user, Map.of())
            .values()
            .stream();
    }

    @Override
    public Optional<StoredTask> lookup(User user, JobId jobId) {
        return Optional.ofNullable(registeredTasks.get(user))
            .map(userTasks -> userTasks.get(jobId));
    }

    @Override
    public long ongoingTaskCount() {
        return registeredTasks.values().stream()
            .flatMap(taskPerJob -> taskPerJob.values().stream())
            .filter(task -> task.task().status() == Status.PENDING || task.task().status() == Status.RUNNING)
            .count();
    }

    @Override
    public void addListener(TaskStoreListener listener) {
        this.listeners.add(listener);
    }
}
