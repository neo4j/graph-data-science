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
package org.neo4j.gds.leiden;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;

class NodesQueue {

    private final HugeLongArrayQueue queue;
    private final BitSet nodeInQueue;

    NodesQueue(long nodeCount) {
        this.queue = HugeLongArrayQueue.newQueue(nodeCount);
        nodeInQueue = new BitSet(nodeCount);
    }

    void add(long nodeId) {
        assert !contains(nodeId) : "Can't add element more than once";
        queue.add(nodeId);
        nodeInQueue.set(nodeId);
    }

    long remove() {
        var nodeId = queue.remove();
        nodeInQueue.flip(nodeId);
        return nodeId;
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    boolean contains(long nodeId) {
        return nodeInQueue.get(nodeId);
    }
}
