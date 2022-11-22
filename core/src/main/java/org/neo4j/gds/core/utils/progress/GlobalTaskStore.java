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

import org.jetbrains.annotations.NotNull;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class GlobalTaskStore implements TaskStore, ThrowingFunction<Context, TaskRegistryFactory, ProcedureException> {

    private final Map<String, Map<JobId, Task>> registeredTasks;

    public GlobalTaskStore() {
        this.registeredTasks = new ConcurrentHashMap<>();
    }

    @Override
    public void store(String username, JobId jobId, Task task) {
        this.registeredTasks
            .computeIfAbsent(username, __ -> new ConcurrentHashMap<>())
            .put(jobId, task);
    }

    @Override
    public void remove(String username, JobId jobId) {
        if (this.registeredTasks.containsKey(username)) {
            this.registeredTasks.get(username).remove(jobId);
        }
    }

    @Override
    public @NotNull Map<JobId, Task> query(String username) {
        return registeredTasks.getOrDefault(username, Map.of());
    }

    @Override
    public Optional<Task> query(String username, JobId jobId) {
        if (registeredTasks.containsKey(username)) {
            return Optional.ofNullable(registeredTasks.get(username).get(jobId));
        }
        return Optional.empty();
    }

    @Override
    public Stream<Task> taskStream() {
        return registeredTasks.values().stream()
            .map(Map::values)
            .flatMap(Collection::stream);
    }

    @Override
    public TaskRegistryFactory apply(Context context) throws ProcedureException {
        var username = Neo4jProxy.username(context.securityContext().subject());
        return new LocalTaskRegistryFactory(username, this);
    }

    @Override
    public boolean isEmpty() {
        return registeredTasks
            .values()
            .stream()
            .allMatch(Map::isEmpty);
    }

    @Override
    public long tasksCount() {
        return registeredTasks.values().stream().mapToLong(Map::size).sum();
    }
}
