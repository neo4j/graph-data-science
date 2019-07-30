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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;

import org.neo4j.graphalgo.core.huge.HugeAdjacencyList;
import org.neo4j.graphalgo.core.huge.HugeAdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.graphalgo.core.huge.loader.AdjacencyCompression.writeDegree;

class HugeAdjacencyBuilder {

    private final HugeAdjacencyListBuilder adjacency;

    private ReentrantLock lock;
    private HugeAdjacencyListBuilder.Allocator allocator;
    private HugeAdjacencyListBuilder.Allocator weightAllocator;
    private HugeAdjacencyOffsets globalOffsets;
    private long[] offsets;
    private long[] weightOffsets;

    private final AllocationTracker tracker;

    HugeAdjacencyBuilder(AllocationTracker tracker) {
        adjacency = HugeAdjacencyListBuilder.newBuilder(tracker);
        this.tracker = tracker;
    }

    HugeAdjacencyBuilder(
            HugeAdjacencyListBuilder adjacency,
            HugeAdjacencyListBuilder.Allocator allocator,
            long[] offsets,
            AllocationTracker tracker) {
        this.adjacency = adjacency;
        this.allocator = allocator;
        this.offsets = offsets;
        this.tracker = tracker;
        this.lock = new ReentrantLock();
    }

    final HugeAdjacencyBuilder threadLocalCopy(long[] offsets, boolean loadDegrees) {
        if (loadDegrees) {
            return new HugeAdjacencyBuilder(
                    adjacency,
                    adjacency.newAllocator(),
                    offsets,
                    tracker);
        }
        return new NoDegreeHAB(adjacency, adjacency.newAllocator(), offsets, tracker);
    }

    final void prepare() {
        allocator.prepare();
    }

    final void setGlobalOffsets(HugeAdjacencyOffsets globalOffsets) {
        this.globalOffsets = globalOffsets;
    }

    final void lock() {
        lock.lock();
    }

    final void unlock() {
        lock.unlock();
    }

    final int applyVariableDeltaEncoding(
            CompressedLongArray array,
            LongsRef buffer,
            int localId) {
        byte[] storage = array.storage();
        AdjacencyCompression.copyFrom(buffer, array);
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);
        long address = copyIds(storage, requiredBytes, degree);
        offsets[localId] = address;
        array.release();
        return degree;
    }

    final int applyVariableDeltaEncodingWithWeights(
            CompressedLongArray array,
            LongsRef buffer,
            int localId) {
        byte[] storage = array.storage();
        double[] weights = array.weights();
        AdjacencyCompression.copyFrom(buffer, array);
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, weights);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);
        int weightsRequiredBytes = degree * Double.BYTES;

        offsets[localId] = copyIds(storage, requiredBytes, degree);
        weightOffsets[localId] = copyWeights(weights, weightsRequiredBytes, degree);
        array.release();
        return degree;
    }

    private synchronized long copyIds(byte[] targets, int requiredBytes, int degree) {
        // sizeOf(degree) + compression bytes
        long address = allocator.allocate(4 + requiredBytes);
        int offset = allocator.offset;
        offset = writeDegree(allocator.page, offset, degree);
        System.arraycopy(targets, 0, allocator.page, offset, requiredBytes);
        allocator.offset = (offset + requiredBytes);
        return address;
    }

    private synchronized long copyWeights(double[] weights, int requiredBytes, int degree) {
        long address = weightAllocator.allocate(requiredBytes);
        int offset = weightAllocator.offset;
        ByteBuffer
                .wrap(weightAllocator.page, offset, requiredBytes)
                .order(ByteOrder.BIG_ENDIAN)
                .asDoubleBuffer()
                .put(weights, 0, degree);
        weightAllocator.offset = (offset + requiredBytes);
        return address;
    }

    int degree(int localId) {
        return (int) offsets[localId];
    }

    static Graph apply(
            final AllocationTracker tracker,
            final IdMap idMapping,
            final HugeWeightMapping weights,
            final Map<String, HugeWeightMapping> nodeProperties,
            final HugeAdjacencyBuilder inAdjacency,
            final HugeAdjacencyBuilder outAdjacency,
            final long relationshipCount) {

        HugeAdjacencyList outAdjacencyList = null;
        HugeAdjacencyOffsets outOffsets = null;
        if (outAdjacency != null) {
            outAdjacencyList = outAdjacency.adjacency.build();
            outOffsets = outAdjacency.globalOffsets;
        }
        HugeAdjacencyList inAdjacencyList = null;
        HugeAdjacencyOffsets inOffsets = null;
        if (inAdjacency != null) {
            inAdjacencyList = inAdjacency.adjacency.build();
            inOffsets = inAdjacency.globalOffsets;
        }

        HugeAdjacencyList outWeights = null;
        HugeAdjacencyOffsets outWeightOffsets = null;

        return new HugeGraph(
                tracker, idMapping, nodeProperties, relationshipCount,
                inAdjacencyList, outAdjacencyList, inOffsets, outOffsets
        );
    }

    private static final class NoDegreeHAB extends HugeAdjacencyBuilder {
        private NoDegreeHAB(
                HugeAdjacencyListBuilder adjacency,
                HugeAdjacencyListBuilder.Allocator allocator,
                long[] offsets,
                AllocationTracker tracker) {
            super(adjacency, allocator, offsets, tracker);
        }

        @Override
        int degree(final int localId) {
            return Integer.MAX_VALUE;
        }
    }
}
