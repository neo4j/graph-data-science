/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.IntStream;


final class Tasks extends AbstractCollection<Runnable> {
    private final AtomicInteger started;
    private final AtomicInteger running;
    private final AtomicInteger requested;
    private final LongAccumulator maxRunning;
    private int size;
    private final long parkNanos;

    Tasks(int size) {
        this.size = size;
        started = new AtomicInteger();
        running = new AtomicInteger();
        requested = new AtomicInteger();
        maxRunning = new LongAccumulator(Long::max, Long.MIN_VALUE);
        parkNanos = 0;
    }

    Tasks(int size, int runtimeMillis) {
        this.size = size;
        started = new AtomicInteger();
        running = new AtomicInteger();
        requested = new AtomicInteger();
        maxRunning = new LongAccumulator(Long::max, Long.MIN_VALUE);
        parkNanos = TimeUnit.MILLISECONDS.toNanos(runtimeMillis);
    }

    @Override
    public Iterator<Runnable> iterator() {
        started.set(0);
        running.set(0);
        requested.set(0);
        maxRunning.reset();
        return new TrackingIterator<>(IntStream.range(0, size).mapToObj($ -> newTask()).iterator());
    }

    @Override
    public int size() {
        return size;
    }

    Tasks sized(int newSize) {
        size = newSize;
        return this;
    }

    int started() {
        return started.get();
    }

    int maxRunning() {
        return (int) maxRunning.get();
    }

    int requested() {
        return requested.get();
    }

    void run(Consumer<Tasks> block) {
        block.accept(this);
    }

    private Runnable newTask() {
        return () -> {
            maxRunning.accumulate(running.incrementAndGet());
            started.incrementAndGet();
            LockSupport.parkNanos(parkNanos);
            running.decrementAndGet();
        };
    }

    private final class TrackingIterator<T> implements Iterator<T> {
        private final Iterator<T> it;

        private TrackingIterator(Iterator<T> it) {
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            requested.incrementAndGet();
            return it.next();
        }
    }
}
