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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class GlobalUserLogStore implements UserLogStore, ThrowingFunction<Context, UserLogRegistryFactory, ProcedureException> {
    public static final int MOST_RECENT = 10;

    private final Map<String, ConcurrentSkipListMap<Task, List<String>>> registeredMessages;

    public GlobalUserLogStore() {

        this.registeredMessages = new ConcurrentHashMap<>();

    }

    public Stream<UserLogEntry> query(String username) {

        var tasksFromUsername = registeredMessages.getOrDefault(username, null);
        if (tasksFromUsername == null)
            return Stream.empty();
        return tasksFromUsername.entrySet().stream().flatMap(GlobalUserLogStore::formEntryToUserLog);


    }

    private static Stream<UserLogEntry> formEntryToUserLog(Map.Entry<Task, List<String>> entry) {
        return entry.getValue().stream().map(message -> new UserLogEntry(entry.getKey(), message));
    }

    public void addUserLogMessage(String username, Task taskId, String message) {

        boolean ignored = false;
        var usernameMap = this.registeredMessages.getOrDefault(username, null);
        Task leastRecentCachedTask = null;
        if (usernameMap != null)
            leastRecentCachedTask = usernameMap.firstKey();
        else {
            this.registeredMessages
                .computeIfAbsent(
                    username,
                    __ -> new ConcurrentSkipListMap<>(Comparator.comparingLong(Task::startTime))
                );
            usernameMap = this.registeredMessages.get(username);
        }
        // if the current task is older, than the oldest in the cache we can ignore it (also works if  cache changes)
        if (leastRecentCachedTask != null && leastRecentCachedTask.startTime() > taskId.startTime()) {
            ignored = true;
        }
        //otherwise, add the message
        if (!ignored) {

            usernameMap
                .computeIfAbsent(taskId, __ -> Collections.synchronizedList(new ArrayList<>()))
                .add(message);

            if (usernameMap.size() > MOST_RECENT) {
                usernameMap.pollFirstEntry();
            }
        }
    }

    @Override
    public UserLogRegistryFactory apply(Context context) throws ProcedureException {
        var username = Neo4jProxy.username(context.securityContext().subject());
        return new LocalUserLogRegistryFactory(username, this);
    }

}


