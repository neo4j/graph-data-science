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
package org.neo4j.graphalgo.beta.pregel;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Queue;

public class Messages implements Iterable<Double> {

    private final MessageIterator iterator;

    Messages(MessageIterator iterator) {
        this.iterator = iterator;
    }

    @NotNull
    @Override
    public Iterator<Double> iterator() {
        return iterator;
    }

    public boolean isEmpty() {
        return iterator.isEmpty();
    }

    public abstract static class MessageIterator implements Iterator<Double> {

        Queue<Double> queue;

        void init(@Nullable Queue<Double> queue) {
            this.queue = queue;
        }

        @Override
        public Double next() {
            return queue.poll();
        }

        public abstract boolean isEmpty();

        static class Sync extends MessageIterator {
            private boolean reachedEnd = false;

            @Override
            void init(@Nullable Queue<Double> queue) {
                super.init(queue);
                reachedEnd = false;
            }

            @Override
            public boolean hasNext() {
                if (queue == null || reachedEnd) {
                    return false;
                }

                if (Double.isNaN(queue.peek())) {
                    queue.poll();
                    reachedEnd = true;
                    return false;
                }

                return true;
            }

            @Override
            public boolean isEmpty() {
                return queue == null || queue.isEmpty() || reachedEnd || Double.isNaN(queue.peek());
            }
        }

        static class Async extends MessageIterator {
            @Override
            public boolean hasNext() {
                if (queue == null) {
                    return false;
                }
                return (queue.peek()) != null;
            }


            @Override
            public boolean isEmpty() {
                return queue == null || queue.isEmpty();
            }
        }
    }
}
