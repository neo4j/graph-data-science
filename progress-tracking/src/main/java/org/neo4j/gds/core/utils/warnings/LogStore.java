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

import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

/**
 * It is understood that these tasks and their messages belong to a single user, but that is handled elsewhere.
 * We cap the number of tasks tracked per user. Tasks are ordered by start time, and with the cap we do FIFO semantics.
 */
class LogStore {
    /**
     * We track 100 tasks per user by default.
     * Note that each task can have an unbounded number of messages, there is no cap there yet.
     */
    private static final int DEFAULT_CAPACITY = 100;

    /**
     * This is important. You can have two tasks with same start time, for example if they are not yet started.
     * It is not strictly great, ideally tasks would have some unique identifier, or something.
     * Like, what if you started two of the same job at the same time innit.
     * We shall solve that another day.
     */
    private static final Comparator<Task> TASK_COMPARATOR = Comparator
        .comparingLong(Task::startTime)
        .thenComparing(Task::description);

    private final ConcurrentSkipListMap<Task, Queue<String>> messages = new ConcurrentSkipListMap<>(TASK_COMPARATOR);

    private final int capacity;

    LogStore(int capacity) {
        this.capacity = capacity;
    }

    public LogStore() {
        this(DEFAULT_CAPACITY);
    }

    void addLogMessage(Task task, String message) {
        getMessageList(task).add(message);

        if (messages.size() > capacity) {
            synchronized (messages) {
                if (messages.size() > capacity) messages.pollFirstEntry();
            }
        }
    }

    Stream<Map.Entry<Task, Queue<String>>> stream() {
        return messages.entrySet().stream();
    }

    /**
     * Can we get away with anything less than {@link java.util.concurrent.ConcurrentLinkedQueue}?
     * You could have multiple things adding entries while multiple things iterate over it.
     * Won't ever happen in real life but in _principle_.
     * And I'm not even really concerned with the results, more the broken pointers one would encounter.
     */
    private Queue<String> getMessageList(Task task) {
        return messages.computeIfAbsent(task, __ -> new ConcurrentLinkedQueue<>());
    }
}
