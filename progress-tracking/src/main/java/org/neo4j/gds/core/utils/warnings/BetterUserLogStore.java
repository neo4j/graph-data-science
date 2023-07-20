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

import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class BetterUserLogStore implements UserLogStore {
    private final ConcurrentHashMap<String, LogStore> logStores = new ConcurrentHashMap<>();

    @Override
    public void addUserLogMessage(String username, Task task, String message) {
        var logStore = getUserLogStore(username);

        logStore.addLogMessage(task, message);
    }

    @Override
    public Stream<UserLogEntry> query(String username) {
        var logStore = getUserLogStore(username);

        return logStore.stream().flatMap(BetterUserLogStore::taskWithMessagesToUserLogEntryStream);
    }

    private LogStore getUserLogStore(String username) {
        return logStores.computeIfAbsent(username, __ -> new LogStore());
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
