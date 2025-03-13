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

import org.neo4j.gds.core.utils.progress.tasks.Status;
import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class PerDatabaseTaskStore extends ObservableTaskStore {
    private final Map<String, Map<JobId, UserTask>> registeredTasks;

    public PerDatabaseTaskStore(Duration retentionPeriod) {
        super(new HashSet<>());
        this.registeredTasks = new ConcurrentHashMap<>();

        this.addListener(new TaskStoreCleaner(this, retentionPeriod));
    }

    @Override
    protected UserTask storeUserTask(String username, JobId jobId, Task task) {
        var userTask = new UserTask(username, jobId, task);
        this.registeredTasks
            .computeIfAbsent(username, __ -> new ConcurrentHashMap<>())
            .put(jobId, userTask);

        return userTask;
    }

    @Override
    protected Optional<UserTask> removeUserTask(String username, JobId jobId) {
        return Optional.ofNullable(this.registeredTasks.get(username))
            .map(userTasks -> userTasks.remove(jobId));
    }

    @Override
    public Stream<UserTask> query() {
        return registeredTasks
            .entrySet()
            .stream()
            .flatMap(tasksPerUsers -> tasksPerUsers
                .getValue()
                .values()
                .stream());
    }

    @Override
    public Stream<UserTask> query(JobId jobId) {
        return query().filter(userTask -> userTask.jobId().equals(jobId));
    }

    @Override
    public Stream<UserTask> query(String username) {
        return registeredTasks
            .getOrDefault(username, Map.of())
            .values()
            .stream();
    }

    @Override
    public Optional<UserTask> query(String username, JobId jobId) {
        return Optional.ofNullable(registeredTasks.get(username))
            .map(userTasks -> userTasks.get(jobId));
    }

    @Override
    public long ongoingTaskCount() {
        return registeredTasks.values().stream()
            .flatMap(taskPerJob -> taskPerJob.values().stream())
            .filter(task -> task.task().status() == Status.PENDING || task.task().status() == Status.RUNNING)
            .count();
    }

}
