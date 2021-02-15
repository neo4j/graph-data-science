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

import org.neo4j.graphalgo.core.compress.AdjacencyCompressor;
import org.neo4j.graphalgo.core.compress.LongArrayBuffer;

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
        if (array.hasWeights()) {
            return applyVariableDeltaEncodingWithWeights(array, buffer, nodeId);
        } else {
            return applyVariableDeltaEncodingWithoutWeights(array, buffer, nodeId);
        }
    }

    private int applyVariableDeltaEncodingWithoutWeights(
        CompressedLongArray array,
        LongArrayBuffer buffer,
        long nodeId
    ) {
        return adjacencyCompressor.compress(nodeId, array, buffer);

//
//        byte[] storage = array.storage();
//        AdjacencyCompression.copyFrom(buffer, array);
//        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, aggregations[0]);
//        int requiredBytes = AdjacencyCompression.compress(buffer, storage);
//        long address = copyIds(storage, requiredBytes, degree);
//        adjacencyOffsets[localId] = address;
//        array.release();
//        return degree;
    }


    private int applyVariableDeltaEncodingWithWeights(
        CompressedLongArray array,
        LongArrayBuffer buffer,
        long nodeId
    ) {
        return adjacencyCompressor.compress(nodeId, array, buffer);

//        byte[] semiCompressedBytesDuringLoading = array.storage();
//        long[][] uncompressedWeightsPerProperty = array.weights();
//
//        // uncompressed semiCompressed into full uncompressed long[] (in buffer)
//        // ordered by whatever order they've been read
//        AdjacencyCompression.copyFrom(buffer, array);
//        // buffer contains uncompressed, unsorted target list
//
//        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, uncompressedWeightsPerProperty, aggregations, noAggregation);
//        // targets are sorted and delta encoded
//        // buffer contains sorted target list
//        // values are delta encoded except for the first one
//        // values are still uncompressed
//
//
//        int requiredBytes = compression.bytesFor(buffer);
//
//
//        int requiredBytes = AdjacencyCompression.compress(buffer, semiCompressedBytesDuringLoading);
//        // values are now vlong encoded in the array storage (semiCompressed)
//
//        adjacencyOffsets[localId] = copyIds(semiCompressedBytesDuringLoading, requiredBytes, degree);
//        copyProperties(uncompressedWeightsPerProperty, degree, localId, propertyOffsets);
//        // values are in the final adjacency list
//
//        array.release();
//        return degree;
    }

//    private long copyIds(byte[] targets, int requiredBytes, int degree) {
//        // sizeOf(degree) + compression bytes
//        var slice = adjacencyAllocator.allocate(Integer.BYTES + requiredBytes);
//        slice.writeInt(degree);
//        slice.insert(targets, 0, requiredBytes);
//        return slice.address();
//    }

//    private void copyProperties(long[][] properties, int degree, int localId, long[][] offsets) {
//        for (int i = 0; i < properties.length; i++) {
//            long[] property = properties[i];
//            var propertiesAllocator = propertiesAllocators[i];
//            long address = copyProperties(property, degree, propertiesAllocator);
//            offsets[i][localId] = address;
//        }
//    }

//    private long copyProperties(long[] properties, int degree, AdjacencyListAllocator propertiesAllocator) {
//        int requiredBytes = degree * Long.BYTES;
//        var slice = propertiesAllocator.allocate(Integer.BYTES /* degree */ + requiredBytes);
//        slice.writeInt(degree);
//        int offset = slice.offset();
//        ByteBuffer
//            .wrap(slice.page(), offset, requiredBytes)
//            .order(ByteOrder.LITTLE_ENDIAN)
//            .asLongBuffer()
//            .put(properties, 0, degree);
//        slice.bytesWritten(requiredBytes);
//        return slice.address();
//    }
}
