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
package org.neo4j.gds.core.pagecached;

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.CompressedLongArray;
import org.neo4j.graphalgo.core.loading.VarLongEncoding;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

class ThreadLocalRelationshipsBuilder {

    private final ReentrantLock lock;
    private final AdjacencyListBuilder.Allocator adjacencyAllocator;
    private final long[] adjacencyOffsets;

    ThreadLocalRelationshipsBuilder(
        AdjacencyListBuilder.Allocator adjacencyAllocator,
        long[] adjacencyOffsets
    ) {
        this.adjacencyAllocator = adjacencyAllocator;
        this.adjacencyOffsets = adjacencyOffsets;
        this.lock = new ReentrantLock();
    }

    final void prepare() throws IOException {
        adjacencyAllocator.prepare();
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
        adjacencyAllocator.close();
    }

    int applyVariableDeltaEncoding(
        CompressedLongArray array,
        LongsRef buffer,
        int localId
    ) throws IOException {
        return applyVariableDeltaEncodingWithoutWeights(array, buffer, localId);
    }

    private int applyVariableDeltaEncodingWithoutWeights(
        CompressedLongArray array,
        LongsRef buffer,
        int localId
    ) throws IOException {
        byte[] storage = array.storage();
        AdjacencyCompression.copyFrom(buffer, array);

        var degree = AdjacencyCompression.applyDeltaEncoding(buffer, Aggregation.NONE);
        AdjacencyCompression.writeBigEndianInt(storage, 0, degree);
        var requiredBytes = VarLongEncoding.encodeVLongs(
            buffer.longs,
            buffer.length,
            storage,
            4
        );

        long address = adjacencyAllocator.insert(storage, 0, requiredBytes);
        adjacencyOffsets[localId] = address;
        array.release();
        return degree;
    }
}
