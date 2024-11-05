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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class PerDatabaseTaskStore implements TaskStore {
    private final Map<String, Map<JobId, Task>> registeredTasks;
    private final List<TaskStoreListener> listeners;

    public PerDatabaseTaskStore() {
        this.registeredTasks = new ConcurrentHashMap<>();
        this.listeners = new ArrayList<>();
    }

    @Override
    public void store(String username, JobId jobId, Task task) {
        listeners.forEach(TaskStoreListener::onTaskStoreUpdate);
        this.registeredTasks
            .computeIfAbsent(username, __ -> new ConcurrentHashMap<>())
            .put(jobId, task);
    }

    @Override
    public void remove(String username, JobId jobId) {
        listeners.forEach(TaskStoreListener::onTaskStoreUpdate);
        if (this.registeredTasks.containsKey(username)) {
            this.registeredTasks.get(username).remove(jobId);
        }
    }

    @Override
    public Stream<UserTask> query() {
        return registeredTasks
            .entrySet()
            .stream()
            .flatMap(tasksPerUsers -> tasksPerUsers
                .getValue()
                .entrySet()
                .stream()
                .map(jobTask -> new UserTask(tasksPerUsers.getKey(), jobTask.getKey(), jobTask.getValue())));
    }

    @Override
    public Stream<UserTask> query(JobId jobId) {
        return query().filter(userTask -> userTask.jobId().equals(jobId));
    }

    @Override
    public Stream<UserTask> query(String username) {
        return registeredTasks
            .getOrDefault(username, Map.of())
            .entrySet()
            .stream()
            .map(jobIdTask -> new UserTask(username, jobIdTask.getKey(), jobIdTask.getValue()));
    }

    @Override
    public Optional<UserTask> query(String username, JobId jobId) {
        if (registeredTasks.containsKey(username)) {
            return Optional
                .ofNullable(registeredTasks.get(username).get(jobId))
                .map(task -> new UserTask(username, jobId, task));
        }
        return Optional.empty();
    }

    @Override
    public boolean isEmpty() {
        return registeredTasks
            .values()
            .stream()
            .allMatch(Map::isEmpty);
    }

    @Override
    public long taskCount() {
        return registeredTasks.values().stream().mapToLong(Map::size).sum();
    }

    @Override
    public synchronized void addListener(TaskStoreListener listener) {
        this.listeners.add(listener);
    }
}
