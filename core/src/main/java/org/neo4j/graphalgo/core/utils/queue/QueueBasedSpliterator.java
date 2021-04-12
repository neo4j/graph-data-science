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
package org.neo4j.graphalgo.core.utils.queue;

import org.neo4j.graphalgo.core.utils.TerminationFlag;

import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;

public class QueueBasedSpliterator<T> implements Spliterator<T> {
    private final BlockingQueue<T> queue;
    private final T tombstone;
    private T entry;
    private final TerminationFlag terminationGuard;
    private final int timeoutInSeconds;

    public QueueBasedSpliterator(BlockingQueue<T> queue, T tombstone, TerminationFlag terminationGuard, int timeoutInSeconds) {
        this.queue = queue;
        this.tombstone = tombstone;
        this.terminationGuard = terminationGuard;
        this.timeoutInSeconds = timeoutInSeconds;
        entry = poll();
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (isEnd()) {
            return false;
        }
        terminationGuard.assertRunning();
        action.accept(entry);
        entry = poll();
        return !isEnd();
    }

    private boolean isEnd() {
        return entry == null || entry == tombstone;
    }

    private T poll() {
        try {
            return queue.poll(timeoutInSeconds, SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public Spliterator<T> trySplit() {
        return null;
    }

    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    public int characteristics() {
        return NONNULL;
    }
}
