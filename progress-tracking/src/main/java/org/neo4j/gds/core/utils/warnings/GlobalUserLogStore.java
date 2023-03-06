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
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class GlobalUserLogStore implements UserLogStore, ThrowingFunction<Context, UserLogRegistryFactory, ProcedureException> {
    public static final int MOST_RECENT = 100;

    private final Map<String, ConcurrentSkipListMap<Task, List<String>>> registeredMessages;

    public GlobalUserLogStore() {
        this.registeredMessages = new ConcurrentHashMap<>();
    }

    public Stream<UserLogEntry> query(String username) {
        if (registeredMessages.containsKey(username)) {
            return registeredMessages.get(username).entrySet().stream().flatMap(GlobalUserLogStore::fromEntryToUserLog);
        }
        return Stream.empty();
    }

    private static Stream<UserLogEntry> fromEntryToUserLog(Map.Entry<Task, List<String>> entry) {
        return entry.getValue().stream().map(message -> new UserLogEntry(entry.getKey(), message));
    }

    private synchronized void pollLeastRecentElement(String username) {
        var usernameMap = this.registeredMessages.get(username);

        //because this is synchronized, this will keep the usernameMap with exactly MOST_RECENT elements
        if (usernameMap.size() > MOST_RECENT) {
            usernameMap.pollFirstEntry();
        }
    }

    private ConcurrentSkipListMap<Task, List<String>> getUserStore(String username) {
        return registeredMessages.computeIfAbsent(
            username,
            __ -> new ConcurrentSkipListMap<>(Comparator.comparingLong(Task::startTime))
        );
    }


    private boolean shouldConsiderTask(SortedMap<Task, List<String>> usernameMap, Task taskId) {
        if (usernameMap.size() < MOST_RECENT) {
            return true;
        } else {
            var leastRecentCachedTask = usernameMap.firstKey();
            return leastRecentCachedTask.startTime() <= taskId.startTime();
        }
    }

    public void addUserLogMessage(String username, Task taskId, String message) {
        var usernameMap = getUserStore(username);

        if (shouldConsiderTask(usernameMap, taskId)) {
            AtomicBoolean addedInStore = new AtomicBoolean();
            usernameMap
                .computeIfAbsent(taskId, __ -> {
                    addedInStore.set(true);
                    return Collections.synchronizedList(new ArrayList<>());
                })
                .add(message);

            //check if something needs to potentially  be removed
            if (addedInStore.get() && usernameMap.size() > MOST_RECENT) {
                pollLeastRecentElement(username);
            }
        }
    }

    @Override
    public UserLogRegistryFactory apply(Context context) throws ProcedureException {
        var username = Neo4jProxy.username(context.securityContext().subject());
        return new LocalUserLogRegistryFactory(username, this);
    }

}


