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
package org.neo4j.gds.maxflow;

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class AtomicWorkingSet {
    private final HugeLongArray workingSet;
    private final AtomicLong index;
    private final AtomicLong size;

    public AtomicWorkingSet(long capacity) {
        workingSet = HugeLongArray.newArray(capacity);
        index = new AtomicLong(0);
        size = new AtomicLong(0);
    }

    boolean isEmpty() {
        return size.get() == index.get();
    }

    void resetIdx() {
        index.set(0L);
    }

    void reset() {
        resetIdx();
        size.set(0L);
    }

    long size() {
        return size.get();
    }

    void push(long value) {
        var idx = size.getAndIncrement();
        workingSet.set(idx, value);
    }

    void batchPush(HugeLongArrayQueue queue) {
        long idx = size.getAndAdd(queue.size());
        while (!queue.isEmpty()) {
            var node = queue.remove();
            workingSet.set(idx++, node);
        }
    }

    long getAndAdd(long batchSize) {
        return index.getAndAdd(batchSize);
    }

    long unsafePeek(long idx) {
        return workingSet.get(idx);
    }

    long pop() {
        var idx = index.getAndIncrement();
        if (idx < size.get()) {
            return workingSet.get(idx);
        } else {
//            index.decrementAndGet(); //undo increment
            return -1L;
        }
    }

    void consumeBatch(long from, long to, Consumer<Long> consumer) {
        for (long idx = from; idx < to; idx++) {
            long v = unsafePeek(idx);
            consumer.accept(v);
        }
    }
}
