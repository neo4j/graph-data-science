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

import org.neo4j.gds.api.User;
import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class PerDatabaseUserLogStore implements UserLogStore {
    private final ConcurrentHashMap<User, LogStore> logStores = new ConcurrentHashMap<>();

    @Override
    public void addUserLogMessage(User user, Task task, String message) {
        var logStore = getUserLogStore(user);

        logStore.addLogMessage(task, message);
    }

    @Override
    public Stream<UserLogEntry> query(User user) {
        var logStore = getUserLogStore(user);

        return logStore.stream().flatMap(PerDatabaseUserLogStore::taskWithMessagesToUserLogEntryStream);
    }

    private LogStore getUserLogStore(User user) {
        return logStores.computeIfAbsent(user, __ -> new LogStore());
    }

    /**
     * One task with messages turns into several user log entries
     */
    private static Stream<UserLogEntry> taskWithMessagesToUserLogEntryStream(Map.Entry<Task, Queue<String>> taskWithMessages) {
        return taskWithMessages.getValue().stream().map(message ->
            new UserLogEntry(
                taskWithMessages.getKey(),
                message
            )
        );
    }
}
