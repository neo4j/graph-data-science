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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.core.compress.LongArrayBuffer;
import org.neo4j.gds.core.compress.AdjacencyCompressor;

import java.util.concurrent.locks.ReentrantLock;

final class ThreadLocalRelationshipsBuilder {

    private final ReentrantLock lock;
    private final AdjacencyCompressor adjacencyCompressor;

    ThreadLocalRelationshipsBuilder(AdjacencyCompressor adjacencyCompressor) {
        this.lock = new ReentrantLock();
        this.adjacencyCompressor = adjacencyCompressor;
    }

    final void lock() {
        lock.lock();
    }

    final void unlock() {
        lock.unlock();
    }

    final boolean isLockedByCurrentThread() {
        return lock.isHeldByCurrentThread();
    }

    void release() {
        adjacencyCompressor.close();
    }

    int applyVariableDeltaEncoding(
        CompressedLongArray array,
        LongArrayBuffer buffer,
        long nodeId
    ) {
        return adjacencyCompressor.compress(nodeId, array, buffer);
    }
}
