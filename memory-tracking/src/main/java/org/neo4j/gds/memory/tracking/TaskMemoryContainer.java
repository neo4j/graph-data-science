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
package org.neo4j.gds.memory.tracking;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.progress.registration.StoredTask;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

class TaskMemoryContainer {
    private final Map<String, ConcurrentHashMap<JobId, Pair<String, Long>>> memoryInUse = new ConcurrentHashMap<>();
    private final AtomicLong allocatedMemory = new AtomicLong();

    void reserve(String username, String taskName, JobId jobId,long memoryAmount){
        memoryInUse.putIfAbsent(username, new ConcurrentHashMap<>());
        memoryInUse.get(username).put(jobId,Pair.of(taskName,memoryAmount));

        allocatedMemory.addAndGet(memoryAmount);
    }

    long removeTask(StoredTask storedTask) {
        var memPair = memoryInUse.getOrDefault(storedTask.user().getUsername(), new ConcurrentHashMap<>()).remove(storedTask.jobId());
        if (memPair != null) {
            var mem = memPair.getRight();
            allocatedMemory.addAndGet(-mem);
            return mem;
        }
        return allocatedMemory.get();
    }

    long taskReservedMemory() {
        return allocatedMemory.get();
    }

    Stream<UserEntityMemory> listTasks(String username) {
        return memoryInUse
            .getOrDefault(username, new ConcurrentHashMap<>())
            .entrySet()
            .stream()
            .map(
                jobIdPairEntry
                    -> UserEntityMemory.createTask(
                    username,
                    jobIdPairEntry.getValue().getLeft(),
                    jobIdPairEntry.getKey(),
                    jobIdPairEntry.getValue().getRight()
                ));
    }

    Stream<UserEntityMemory> listTasks() {
        return memoryInUse.keySet().stream().flatMap(this::listTasks);
    }

    long memoryOfTasks(String username) {
        return memoryInUse
            .getOrDefault(username, new ConcurrentHashMap<>())
            .values()
            .stream()
            .map(Pair::getRight)
            .reduce(0L, Long::sum);
    }

    Set<String> taskUsers(Set<String> inputUsernames) {
        var usernames = new HashSet<>(inputUsernames);
        usernames.addAll(memoryInUse.keySet());
        return usernames;
    }
}
