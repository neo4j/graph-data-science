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
package org.neo4j.gds.core.utils.warnings;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class GlobalUserLogStore implements UserLogStore, ThrowingFunction<Context, UserLogRegistryFactory, ProcedureException> {
    public static final int MOST_RECENT = 10;

    private final Map<String, Map<Task, List<String>>> registeredWarnings;

    public GlobalUserLogStore() {

        this.registeredWarnings = new ConcurrentHashMap<>();
    }

    public Stream<UserLogEntry> query(String username) {

        var tasksFromUsername = registeredWarnings.getOrDefault(username, Map.of());
        return tasksFromUsername.entrySet().stream().flatMap(GlobalUserLogStore::fromEntryToWarningList);


    }

    private static Stream<UserLogEntry> fromEntryToWarningList(Map.Entry<Task, List<String>> entry) {
        return entry.getValue().stream().map(message -> new UserLogEntry(entry.getKey(), message));
    }

    public void addUserLogMessage(String username, Task taskId, String message) {
        this.registeredWarnings
            .computeIfAbsent(username, __ -> new ConcurrentHashMap<>())
            .computeIfAbsent(taskId, __ -> new ArrayList<>(MOST_RECENT))
            .add(message);
    }

    @Override
    public UserLogRegistryFactory apply(Context context) throws ProcedureException {
        var username = Neo4jProxy.username(context.securityContext().subject());
        return new LocalUserLogRegistryFactory(username, this);
    }
}


