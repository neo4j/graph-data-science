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

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalTaskRegistryMap implements GlobalTaskRegistry, ThrowingFunction<Context, TaskRegistry, ProcedureException> {

    private final Map<String, Map<JobId, Task>> registeredTasks;

    public GlobalTaskRegistryMap() {
        this.registeredTasks = new ConcurrentHashMap<>();
    }

    @Override
    public void registerTask(String username, JobId jobId, Task task) {
        this.registeredTasks
            .computeIfAbsent(username, __ -> new ConcurrentHashMap<>())
            .put(jobId, task);
    }

    @Override
    public void unregisterTask(String username, JobId jobId) {
        if (this.registeredTasks.containsKey(username)) {
            this.registeredTasks.get(username).remove(jobId);
        }
    }

    @Override
    public Map<JobId, Task> query(String username) {
        return registeredTasks.get(username);
    }

    @Override
    public Task query(String username, JobId jobId) {
        if (registeredTasks.containsKey(username)) {
            return registeredTasks.get(username).get(jobId);
        }
        return null;
    }

    @Override
    public TaskRegistry apply(Context context) throws ProcedureException {
        var username = context.securityContext().subject().username();
        return new LocalTaskRegistry(username, this);
    }

    @Override
    public boolean isEmpty() {
        return registeredTasks.isEmpty();
    }
}
