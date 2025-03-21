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
package org.neo4j.gds.core.utils.queue;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.utils.ExceptionUtil;

import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueueBasedSpliterator<T> implements Spliterator<T> {
    private final BlockingQueue<T> queue;
    private final T tombstone;
    private final AtomicReference<Throwable> queuePopulatorError;
    private @Nullable T entry;
    private final TerminationFlag terminationGuard;

    public QueueBasedSpliterator(
        BlockingQueue<T> queue,
        T tombstone,
        TerminationFlag terminationGuard
    ) {
        this(queue, tombstone, new AtomicReference<>(), terminationGuard);
    }

    public QueueBasedSpliterator(
        BlockingQueue<T> queue,
        T tombstone,
        AtomicReference<Throwable> queuePopulatorError,
        TerminationFlag terminationGuard
    ) {
        this.queue = queue;
        this.tombstone = tombstone;
        this.terminationGuard = terminationGuard;
        this.queuePopulatorError = queuePopulatorError;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        entry = poll();
        if (isEnd()) {
            return false;
        }
        action.accept(entry);
        return !isEnd();
    }

    private boolean isEnd() {
        return entry == null || entry == tombstone;
    }

    private @NotNull T poll() {
        T poll;
        do {
            checkForQueuePopulatorError();
            try {
                poll = queue.poll(100, MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ExceptionUtil.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        } while (poll == null);

        return poll;
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

    private void checkForQueuePopulatorError() {
        terminationGuard.assertRunning();
        var throwable = queuePopulatorError.get();
        if (throwable != null) {
            ExceptionUtil.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }
}
