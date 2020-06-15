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
package org.neo4j.graphalgo.core.pagecached;

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.Aggregation;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

class ThreadLocalRelationshipsBuilder {

    private final ReentrantLock lock;
    private final AdjacencyListBuilder.Allocator adjacencyAllocator;
    private final AdjacencyListBuilder.Allocator[] propertiesAllocators;
    private final long[] adjacencyOffsets;
    private final long[][] propertyOffsets;
    private final boolean noAggregation;
    private final Aggregation[] aggregations;

    ThreadLocalRelationshipsBuilder(
        AdjacencyListBuilder.Allocator adjacencyAllocator,
        long[] adjacencyOffsets
    ) {
        this(adjacencyAllocator, null, adjacencyOffsets, null, null);
    }

    ThreadLocalRelationshipsBuilder(
        AdjacencyListBuilder.Allocator adjacencyAllocator,
        AdjacencyListBuilder.Allocator[] propertiesAllocators,
        long[] adjacencyOffsets,
        long[][] propertyOffsets,
        Aggregation[] aggregations
    ) {

        this.aggregations = aggregations;

        this.noAggregation = Stream.of(aggregations).allMatch(aggregation -> aggregation == Aggregation.NONE);

        this.adjacencyAllocator = adjacencyAllocator;
        this.propertiesAllocators = propertiesAllocators;
        this.adjacencyOffsets = adjacencyOffsets;
        this.propertyOffsets = propertyOffsets;
        this.lock = new ReentrantLock();
    }

    final void prepare() throws IOException {
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

        // TODO: implement property loading
//        if (array.hasWeights()) {
//            return applyVariableDeltaEncodingWithWeights(array, buffer, localId);
//        } else {
            return applyVariableDeltaEncodingWithoutWeights(array, buffer, localId);
//        }
    }

    private int applyVariableDeltaEncodingWithoutWeights(
        CompressedLongArray array,
        LongsRef buffer,
        int localId
    ) throws IOException {
        byte[] storage = array.storage();
        AdjacencyCompression.copyFrom(buffer, array);

        var degree = AdjacencyCompression.applyDeltaEncoding(buffer, Aggregation.NONE);
        AdjacencyCompression.writeBEInt(storage, 0, degree);
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
