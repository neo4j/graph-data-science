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
package org.neo4j.gds.core.utils.paged;

import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;

public final class HugeLongArrayQueue {

    private final HugeLongArray array;
    private final long capacity;
    private long head;
    private long tail;

    public static HugeLongArrayQueue newQueue(long capacity) {
        return new HugeLongArrayQueue(HugeLongArray.newArray(capacity + 1));
    }

    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(HugeLongArrayQueue.class)
            .perNode("array", HugeLongArray::memoryEstimation).build();
    }

    private HugeLongArrayQueue(HugeLongArray array) {
        this.head = 0;
        this.tail = 0;
        this.capacity = array.size();
        this.array = array;
    }

    public void add(long v) {
        long newTail = (tail + 1) % capacity;
        if (newTail == head) {
            throw new IndexOutOfBoundsException("Queue is full.");
        }
        array.set(tail, v);
        tail = newTail;
    }

    public long remove() {
        if (isEmpty()) {
            throw new IndexOutOfBoundsException("Queue is empty.");
        }
        long removed = array.get(head);
        head = (head + 1) % capacity;
        return removed;
    }

    public long peek() {
        if (isEmpty()) {
            throw new IndexOutOfBoundsException("Queue is empty.");
        }
        return array.get(head);
    }

    public long size() {
        long diff = tail - head;
        if (diff < 0) {
            diff += capacity;
        }
        return diff;
    }

    public boolean isEmpty() {
        return head == tail;
    }
}
