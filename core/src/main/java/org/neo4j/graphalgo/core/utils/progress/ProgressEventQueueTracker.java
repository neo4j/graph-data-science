/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import java.util.OptionalDouble;
import java.util.Queue;

final class ProgressEventQueueTracker implements ProgressEventTracker {

    private final Queue<LogEvent> queue;
    private final String username;

    ProgressEventQueueTracker(Queue<LogEvent> queue, String username) {
        this.queue = queue;
        this.username = username;
    }

    // MP
    @Override
    public void addLogEvent(
        String id,
        String message
    ) {
        var logEvent = ImmutableLogEvent.of(username, id, message, OptionalDouble.empty());
        this.queue.offer(logEvent);
    }

    // MP
    @Override
    public void addLogEvent(
        String id,
        String message,
        double progress
    ) {

    }

    // MP
    @Override
    public void addLogEvent(
        LogEvent event
    ) {

    }

    @Override
    public void release(String id) {
        queue.offer(ImmutableLogEvent.builder().username(username).id(id).message("").release(true).build());
    }
}
