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
package org.neo4j.graphalgo.core.loading;

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.Aggregation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.graphalgo.core.loading.AdjacencyCompression.writeDegree;

class ThreadLocalRelationshipsBuilder {

    private final ReentrantLock lock;
    private final Aggregation[] aggregations;
    private final AdjacencyListBuilder.Allocator adjacencyAllocator;
    private final AdjacencyListBuilder.Allocator[] propertiesAllocators;
    private final long[] adjacencyOffsets;
    private final long[][] weightOffsets;
    private final boolean noAggregation;

    ThreadLocalRelationshipsBuilder(
            Aggregation[] aggregations,
            AdjacencyListBuilder.Allocator adjacencyAllocator,
            final AdjacencyListBuilder.Allocator[] propertiesAllocators,
            long[] adjacencyOffsets,
            final long[][] weightOffsets) {
        if (aggregations.length == 0) {
            throw new IllegalArgumentException("Needs at least one aggregation");
        }
        this.aggregations = aggregations;
        this.noAggregation = Arrays.stream(aggregations).allMatch(d -> d == Aggregation.NONE);
        this.adjacencyAllocator = adjacencyAllocator;
        this.propertiesAllocators = propertiesAllocators;
        this.adjacencyOffsets = adjacencyOffsets;
        this.weightOffsets = weightOffsets;
        this.lock = new ReentrantLock();
    }

    final void prepare() {
        adjacencyAllocator.prepare();

        for (AdjacencyListBuilder.Allocator weightsAllocator : propertiesAllocators) {
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
            int localId) {
        byte[] storage = array.storage();
        long[][] weights = array.weights();
        AdjacencyCompression.copyFrom(buffer, array);
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, weights, aggregations, noAggregation);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);

        adjacencyOffsets[localId] = copyIds(storage, requiredBytes, degree);
        copyProperties(weights, degree, localId, weightOffsets);

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

    private void copyProperties(long[][] properties, int degree, int localId, long[][] offsets) {
        for (int i = 0; i < properties.length; i++) {
            long[] property = properties[i];
            AdjacencyListBuilder.Allocator propertiesAllocator = propertiesAllocators[i];
            long address = copyProperties(property, degree, propertiesAllocator);
            offsets[i][localId] = address;
        }
    }

    private long copyProperties(long[] properties, int degree, AdjacencyListBuilder.Allocator propertiesAllocator) {
        int requiredBytes = degree * Long.BYTES;
        long address = propertiesAllocator.allocate(Integer.BYTES /* degree */ + requiredBytes);
        int offset = propertiesAllocator.offset;
        offset = writeDegree(propertiesAllocator.page, offset, degree);
        ByteBuffer
                .wrap(propertiesAllocator.page, offset, requiredBytes)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asLongBuffer()
                .put(properties, 0, degree);
        propertiesAllocator.offset = (offset + requiredBytes);
        return address;
    }
}
