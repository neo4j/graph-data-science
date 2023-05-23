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
package org.neo4j.gds.impl.msbfs;

import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class AllShortestPathsStream {
    private AllShortestPathsStream() {}

    static Stream<AllShortestPathsStreamResult> stream(BlockingQueue<AllShortestPathsStreamResult> resultQueue, Runnable closeAction) {
        var spliterator = new ResultSpliterator(resultQueue, closeAction);
        return StreamSupport.stream(spliterator, false).onClose(spliterator::close);
    }

    private static class ResultSpliterator implements Spliterator<AllShortestPathsStreamResult>, AutoCloseable {
        private final BlockingQueue<AllShortestPathsStreamResult> resultQueue;
        private final Runnable closeAction;
        private final AtomicBoolean isClosed;

        ResultSpliterator(
            BlockingQueue<AllShortestPathsStreamResult> resultQueue,
            Runnable closeAction
        ) {
            this.resultQueue = resultQueue;
            this.closeAction = closeAction;
            this.isClosed = new AtomicBoolean(false);
        }

        @Override
        public Spliterator<AllShortestPathsStreamResult> trySplit() {
            // no parallel consumption/splitting possible
            return null;
        }

        @Override
        public void forEachRemaining(Consumer<? super AllShortestPathsStreamResult> action) {
            Objects.requireNonNull(action);
            if (isClosed()) {
                return;
            }
            boolean hasNext;
            do {
                hasNext = this.tryAdvance(action);
            } while (hasNext);
            close();
        }

        @Override
        public boolean tryAdvance(Consumer<? super AllShortestPathsStreamResult> action) {
            Objects.requireNonNull(action);
            if (isClosed()) {
                return false;
            }
            try {
                var result = resultQueue.take();
                if (result != AllShortestPathsStreamResult.DONE) {
                    action.accept(result);
                    return true;
                }
                close();
                return false;
            } catch (InterruptedException e) {
                close();
                // notify JVM of the interrupt
                Thread.currentThread().interrupt();
                return false;
            }
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public long getExactSizeIfKnown() {
            return -1L;
        }

        @Override
        public int characteristics() {
            return Spliterator.NONNULL | Spliterator.IMMUTABLE;
        }

        @Override
        public void close() {
            // the close action like endSubTask is not idempotent
            // we need to only call it once
            var shouldClose = isClosed.compareAndSet(false, true);
            if (shouldClose) {
                closeAction.run();
            }
        }

        private boolean isClosed() {
            return isClosed.get();
        }
    }
}
