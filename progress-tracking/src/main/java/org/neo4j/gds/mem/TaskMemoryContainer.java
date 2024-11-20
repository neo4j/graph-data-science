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
package org.neo4j.gds.mem;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.UserTask;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

class TaskMemoryContainer {

    private final ConcurrentHashMap<String,ConcurrentHashMap<JobId, Pair<String,Long>>> memoryInUse = new ConcurrentHashMap<>();
    private final AtomicLong allocatedMemory = new AtomicLong();
    private static final ConcurrentHashMap<JobId,Pair<String,Long>> EMPTY_HASH_MAP = new ConcurrentHashMap<>();

    void reserve(String username, String taskName, JobId jobId,long memoryAmount){
        memoryInUse.putIfAbsent(username, new ConcurrentHashMap<>());
        memoryInUse.get(username).put(jobId,Pair.of(taskName,memoryAmount));

        allocatedMemory.addAndGet(memoryAmount);
    }

    long removeTask(UserTask task){
         var mem=  memoryInUse.getOrDefault(task.username(), EMPTY_HASH_MAP).remove(task.jobId()).getRight();
         allocatedMemory.addAndGet(mem);
         return  mem;
    }

    long taskReservedMemory(){
        return  allocatedMemory.get();
    }

    Stream<UserEntityMemory> listTasks(String user){
        return  memoryInUse
            .getOrDefault(user, EMPTY_HASH_MAP)
            .values()
            .stream()
            .map(stringLongPair -> new UserEntityMemory(user, stringLongPair.getLeft(), stringLongPair.getRight()));
    }

    Stream<UserEntityMemory> listTasks(){
        return  memoryInUse.keySet().stream().flatMap(this::listTasks);
    }

}
