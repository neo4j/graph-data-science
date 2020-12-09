/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.progress;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.utils.RenamesCurrentThread;
import org.neo4j.internal.kernel.api.security.AuthSubject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.Collections.emptyMap;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;

public final class ProgressEventConsumer implements Runnable {

    private final Queue<LogEvent> queue;
    private final Map<String, Map<String, List<LogEvent>>> events;
    private volatile boolean running;
    private volatile @Nullable Thread runningThread;

    public ProgressEventConsumer(Queue<LogEvent> queue) {
        this.queue = queue;
        events = new HashMap<>();
        running = false;
    }

    public List<LogEvent> query(String username) {
        return events
            .getOrDefault(username, emptyMap())
            .values()
            .stream()
            .filter(not(List::isEmpty))
            .map(items -> items.get(items.size() - 1))
            .collect(toList());
    }

    @Override
    public void run() {
        try (RenamesCurrentThread.Revert ignored = RenamesCurrentThread.renameThread("progress-event-consumer")) {
            while (running) {
                pollNext();
            }
        }
    }

    /* test-private */ void pollNext() {
        var event = queue.poll();
        while (event == null) {
            if (!running) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            event = queue.poll();
        }

        events
            .computeIfAbsent(AuthSubject.ANONYMOUS.username(), __ -> new HashMap<>())
            .computeIfAbsent(event.id(), __ -> new ArrayList<>())
            .add(event);
    }

    void start() {
        if (running) {
            return;
        }

        running = true;
        runningThread = new Thread(this);
        runningThread.start();
    }

    void stop() throws InterruptedException {
        if (!running || runningThread == null) {
            return;
        }
        running = false;
        runningThread.join();
        runningThread = null;
    }
}
