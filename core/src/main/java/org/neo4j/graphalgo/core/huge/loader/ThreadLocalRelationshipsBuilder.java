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
import org.neo4j.graphalgo.core.DeduplicationStrategy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.graphalgo.core.huge.loader.AdjacencyCompression.writeDegree;

class ThreadLocalRelationshipsBuilder {

    private final ReentrantLock lock;
    private final DeduplicationStrategy[] deduplicationStrategies;
    private final AdjacencyListBuilder.Allocator adjacencyAllocator;
    private final AdjacencyListBuilder.Allocator[] weightsAllocators;
    private final long[] adjacencyOffsets;
    private final long[][] weightOffsets;
    private final boolean noDeduplication;

    ThreadLocalRelationshipsBuilder(
            DeduplicationStrategy[] deduplicationStrategies,
            AdjacencyListBuilder.Allocator adjacencyAllocator,
            final AdjacencyListBuilder.Allocator[] weightsAllocators,
            long[] adjacencyOffsets,
            final long[][] weightOffsets) {
        if (deduplicationStrategies.length == 0) {
            throw new IllegalArgumentException("Needs at least one deduplication strategy");
        }
        this.deduplicationStrategies = deduplicationStrategies;
        noDeduplication = Arrays.stream(deduplicationStrategies).anyMatch(d -> d == DeduplicationStrategy.NONE);
        this.adjacencyAllocator = adjacencyAllocator;
        this.weightsAllocators = weightsAllocators;
        this.adjacencyOffsets = adjacencyOffsets;
        this.weightOffsets = weightOffsets;
        this.lock = new ReentrantLock();
    }

    final void prepare() {
        adjacencyAllocator.prepare();

        for (AdjacencyListBuilder.Allocator weightsAllocator : weightsAllocators) {
            if (weightsAllocator != null) {
                weightsAllocator.prepare();
            }
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
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, deduplicationStrategies[0]);
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
        long[][] weights = array.weights();
        AdjacencyCompression.copyFrom(buffer, array);
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, weights, deduplicationStrategies, noDeduplication);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);

        adjacencyOffsets[localId] = copyIds(storage, requiredBytes, degree);
        copyWeights(weights, degree, localId, weightOffsets);

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

    private void copyWeights(long[][] weights, int degree, int localId, long[][] offsets) {
        for (int i = 0; i < weights.length; i++) {
            long[] weight = weights[i];
            AdjacencyListBuilder.Allocator weightsAllocator = weightsAllocators[i];
            long address = copyWeights(weight, degree, weightsAllocator);
            offsets[i][localId] = address;
        }
    }

    private long copyWeights(long[] weights, int degree, AdjacencyListBuilder.Allocator weightsAllocator) {
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
}
