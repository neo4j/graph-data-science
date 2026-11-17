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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.progress.tasks.Task;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class PerDatabaseTaskStore implements TaskStore {
    private final Map<Pair<User, JobId>, TaskContainer> registeredTasks = new ConcurrentHashMap<>();

    private final Set<TaskStoreListener> listeners = ConcurrentHashMap.newKeySet();

    private PerDatabaseTaskStore() {

    }

    public static PerDatabaseTaskStore create(Duration retentionPeriod) {
        var taskStore = new PerDatabaseTaskStore();

        var taskStoreCleaner = new TaskStoreCleaner(taskStore, retentionPeriod);
        taskStore.addListener(taskStoreCleaner);

        return taskStore;
    }

    @Override
    public void store(User user, JobId jobId, Task task) {
        // compute() is atomic per key, so a concurrent remove() cannot detach the container
        // between looking it up and adding the task to it
        registeredTasks.compute(Pair.of(user, jobId), (__, taskContainer) -> {
            var container = taskContainer == null ? new TaskContainer() : taskContainer;
            container.add(task);
            return container;
        });

        var storedTask = new StoredTask(user, jobId, task);

        listeners.forEach(listener -> listener.onTaskAdded(storedTask));
    }

    @Override
    public void remove(User user, JobId jobId) {
        registeredTasks.remove(Pair.of(user, jobId));
    }

    @Override
    public void markCompleted(User user, JobId jobId) {
        var taskContainer = getTaskContainer(user, jobId);

        var tasks = taskContainer.markAllCompleted();

        AtomicReference<TaskStoreListener> cleanListenerRef = new AtomicReference<>(null);

        listeners.forEach(listener ->
        {
            if (listener instanceof TaskStoreCleaner) {
                cleanListenerRef.set(listener);
            } else {
                tasks.forEach(task -> {
                    var storedTask = new StoredTask(user, jobId, task);
                    listener.onTaskCompleted(storedTask);
                });
            }
        });

        if (cleanListenerRef.get() != null) {
            var listener = cleanListenerRef.get();
            tasks.forEach(task -> listener.onTaskCompleted(new StoredTask(user, jobId, task)));
        }

    }

    @Override
    public Stream<StoredTask> query() {
        return queryWithFilter(__ -> true);
    }

    @Override
    public Stream<StoredTask> query(JobId jobId) {
        return queryWithFilter(entry -> entry.getRight().equals(jobId));
    }

    @Override
    public Stream<StoredTask> query(User user) {
        return queryWithFilter(entry -> entry.getLeft().equals(user));
    }

    @Override
    public Set<StoredTask> lookup(User user, JobId jobId) {
        var taskContainer = getTaskContainer(user, jobId);

        return taskContainer.getAll().stream()
            .map(task -> new StoredTask(user, jobId, task))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public long ongoingTaskCount() {
        return registeredTasks.values().stream()
            .mapToLong(taskContainer -> taskContainer.getAll().stream()
                .filter(task -> ActiveStatuses.Statuses.contains(task.status()))
                .count()
            )
            .sum();
    }

    @Override
    public void addListener(TaskStoreListener listener) {
        this.listeners.add(listener);
    }

    /**
     * This ensures you never have to deal with a null task container.
     * Unknown keys get an empty container that is not added to the registry.
     */
    private TaskContainer getTaskContainer(User user, JobId jobId) {
        var taskContainer = registeredTasks.get(Pair.of(user, jobId));
        return taskContainer == null ? new TaskContainer() : taskContainer;
    }

    /**
     * Number of (user, job) entries currently held; visible for testing.
     */
    int registeredJobCount() {
        return registeredTasks.size();
    }

    private Stream<StoredTask> queryWithFilter(Predicate<Pair<User, JobId>> filter) {
        return registeredTasks.entrySet().stream()
            .filter(entry -> filter.test(entry.getKey()))
            .flatMap(entry -> {
                var key = entry.getKey();
                var value = entry.getValue();

                return value.getAll().stream().map(task -> new StoredTask(key.getKey(), key.getValue(), task));
            });
    }
}
