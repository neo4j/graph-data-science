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
package org.neo4j.graphalgo.core.loading;

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.Aggregation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

class ThreadLocalRelationshipsBuilder {

    private final ReentrantLock lock;
    private final AdjacencyListAllocator adjacencyAllocator;
    private final AdjacencyListAllocator[] propertiesAllocators;
    private final long[] adjacencyOffsets;
    private final long[][] propertyOffsets;
    private final boolean noAggregation;
    private final Aggregation[] aggregations;

    ThreadLocalRelationshipsBuilder(
        AdjacencyListAllocator adjacencyAllocator,
        AdjacencyListAllocator[] propertiesAllocators,
        long[] adjacencyOffsets,
        long[][] propertyOffsets,
        Aggregation[] aggregations
    ) {
        this.adjacencyAllocator = adjacencyAllocator;
        this.propertiesAllocators = propertiesAllocators;
        this.adjacencyOffsets = adjacencyOffsets;
        this.propertyOffsets = propertyOffsets;
        this.aggregations = aggregations;
        this.lock = new ReentrantLock();
        this.noAggregation = Stream.of(aggregations).allMatch(aggregation -> aggregation == Aggregation.NONE);
    }

    final void prepare() {
        adjacencyAllocator.prepare();
        for (var propertiesAllocator : propertiesAllocators) {
            if (propertiesAllocator != null) {
                propertiesAllocator.prepare();
            }
        }
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
        for (var propertiesAllocator : propertiesAllocators) {
            if (propertiesAllocator != null) {
                propertiesAllocator.close();
            }
        }
    }

    int applyVariableDeltaEncoding(
        CompressedLongArray array,
        LongsRef buffer,
        int localId
    ) {
        if (array.hasWeights()) {
            return applyVariableDeltaEncodingWithWeights(array, buffer, localId);
        } else {
            return applyVariableDeltaEncodingWithoutWeights(array, buffer, localId);
        }
    }

    private int applyVariableDeltaEncodingWithoutWeights(
        CompressedLongArray array,
        LongsRef buffer,
        int localId
    ) {
        byte[] storage = array.storage();
        AdjacencyCompression.copyFrom(buffer, array);
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, aggregations[0]);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);
        long address = copyIds(storage, requiredBytes, degree);
        adjacencyOffsets[localId] = address;
        array.release();
        return degree;
    }

    private int applyVariableDeltaEncodingWithWeights(
        CompressedLongArray array,
        LongsRef buffer,
        int localId
    ) {
        byte[] storage = array.storage();
        long[][] weights = array.weights();
        AdjacencyCompression.copyFrom(buffer, array);
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, weights, aggregations, noAggregation);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);
        adjacencyOffsets[localId] = copyIds(storage, requiredBytes, degree);
        copyProperties(weights, degree, localId, propertyOffsets);

        array.release();
        return degree;
    }

    private long copyIds(byte[] targets, int requiredBytes, int degree) {
        // sizeOf(degree) + compression bytes
        var slice = adjacencyAllocator.allocate(Integer.BYTES + requiredBytes);
        slice.writeInt(degree);
        slice.insert(targets, 0, requiredBytes);
        return slice.address();
    }

    private void copyProperties(long[][] properties, int degree, int localId, long[][] offsets) {
        for (int i = 0; i < properties.length; i++) {
            long[] property = properties[i];
            var propertiesAllocator = propertiesAllocators[i];
            long address = copyProperties(property, degree, propertiesAllocator);
            offsets[i][localId] = address;
        }
    }

    private long copyProperties(long[] properties, int degree, AdjacencyListAllocator propertiesAllocator) {
        int requiredBytes = degree * Long.BYTES;
        var slice = propertiesAllocator.allocate(Integer.BYTES /* degree */ + requiredBytes);
        slice.writeInt(degree);
        int offset = slice.offset();
        ByteBuffer
            .wrap(slice.page(), offset, requiredBytes)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asLongBuffer()
            .put(properties, 0, degree);
        slice.bytesWritten(requiredBytes);
        return slice.address();
    }
}
