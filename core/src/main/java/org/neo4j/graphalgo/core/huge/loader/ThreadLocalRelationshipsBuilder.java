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
package org.neo4j.graphalgo.core.huge.loader;

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.graphalgo.core.huge.loader.AdjacencyCompression.writeDegree;

class ThreadLocalRelationshipsBuilder {

    private final ReentrantLock lock;
    private final DuplicateRelationshipsStrategy duplicateRelationshipsStrategy;
    private final HugeAdjacencyListBuilder.Allocator adjacencyAllocator;
    private final HugeAdjacencyListBuilder.Allocator weightsAllocator;
    private final long[] adjacencyOffsets;
    private final long[] weightOffsets;

    ThreadLocalRelationshipsBuilder(
            DuplicateRelationshipsStrategy duplicateRelationshipsStrategy,
            HugeAdjacencyListBuilder.Allocator adjacencyAllocator,
            final HugeAdjacencyListBuilder.Allocator weightsAllocator,
            long[] adjacencyOffsets,
            final long[] weightOffsets) {
        this.duplicateRelationshipsStrategy = duplicateRelationshipsStrategy;
        this.adjacencyAllocator = adjacencyAllocator;
        this.weightsAllocator = weightsAllocator;
        this.adjacencyOffsets = adjacencyOffsets;
        this.weightOffsets = weightOffsets;
        this.lock = new ReentrantLock();
    }

    final void prepare() {
        adjacencyAllocator.prepare();

        if (weightsAllocator != null) {
            weightsAllocator.prepare();
        }
    }

    final void lock() {
        lock.lock();
    }

    final void unlock() {
        lock.unlock();
    }

    int applyVariableDeltaEncoding(
            CompressedLongArray array,
            LongsRef buffer,
            int localId) {

        if (array.hasWeights()) {
            return applyVariableDeltaEncodingWithWeights(array, buffer, localId);
        } else {
            return applyVariableDeltaEncodingWithoutWeights(array, buffer, localId);
        }
    }

    private int applyVariableDeltaEncodingWithoutWeights(
            CompressedLongArray array,
            LongsRef buffer,
            int localId) {
        byte[] storage = array.storage();
        AdjacencyCompression.copyFrom(buffer, array);
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);
        long address = copyIds(storage, requiredBytes, degree);
        adjacencyOffsets[localId] = address;
        array.release();
        return degree;
    }

    private int applyVariableDeltaEncodingWithWeights(
            CompressedLongArray array,
            LongsRef buffer,
            int localId) {
        byte[] storage = array.storage();
        long[] weights = array.weights();
        AdjacencyCompression.copyFrom(buffer, array);
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, weights, duplicateRelationshipsStrategy);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);

        adjacencyOffsets[localId] = copyIds(storage, requiredBytes, degree);
        weightOffsets[localId] = copyWeights(weights, degree);

        array.release();
        return degree;
    }

    private long copyIds(byte[] targets, int requiredBytes, int degree) {
        // sizeOf(degree) + compression bytes
        long address = adjacencyAllocator.allocate(Integer.BYTES + requiredBytes);
        int offset = adjacencyAllocator.offset;
        offset = writeDegree(adjacencyAllocator.page, offset, degree);
        System.arraycopy(targets, 0, adjacencyAllocator.page, offset, requiredBytes);
        adjacencyAllocator.offset = (offset + requiredBytes);
        return address;
    }

    private long copyWeights(long[] weights, int degree) {
        int requiredBytes = degree * Long.BYTES;
        long address = weightsAllocator.allocate(Integer.BYTES /* degree */ + requiredBytes);
        int offset = weightsAllocator.offset;
        offset = writeDegree(weightsAllocator.page, offset, degree);
        ByteBuffer
                .wrap(weightsAllocator.page, offset, requiredBytes)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asLongBuffer()
                .put(weights, 0, degree);
        weightsAllocator.offset = (offset + requiredBytes);
        return address;
    }

    int degree(int localId) {
        return (int) adjacencyOffsets[localId];
    }
}
