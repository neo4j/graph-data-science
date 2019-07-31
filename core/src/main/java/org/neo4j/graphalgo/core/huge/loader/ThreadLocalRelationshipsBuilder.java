package org.neo4j.graphalgo.core.huge.loader;

import org.apache.lucene.util.LongsRef;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.graphalgo.core.huge.loader.AdjacencyCompression.writeDegree;

class ThreadLocalRelationshipsBuilder {

    private final ReentrantLock lock;
    private final HugeAdjacencyListBuilder.Allocator adjacencyAllocator;
    private final HugeAdjacencyListBuilder.Allocator weightsAllocator;
    private final long[] adjacencyOffsets;
    private final long[] weightOffsets;

    ThreadLocalRelationshipsBuilder(
            HugeAdjacencyListBuilder.Allocator adjacencyAllocator,
            final HugeAdjacencyListBuilder.Allocator weightsAllocator,
            long[] adjacencyOffsets,
            final long[] weightOffsets) {
        this.adjacencyAllocator = adjacencyAllocator;
        this.weightsAllocator = weightsAllocator;
        this.adjacencyOffsets = adjacencyOffsets;
        this.weightOffsets = weightOffsets;
        this.lock = new ReentrantLock();
    }

    final void prepare() {
        adjacencyAllocator.prepare();
        weightsAllocator.prepare();
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
        long address = copyValues(storage, requiredBytes, degree, adjacencyAllocator);
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
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, weights);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);

        byte[] compressedWeights = new byte[degree * 10];
        int weightsRequiredBytes = AdjacencyCompression.compress(weights, compressedWeights, degree);

        adjacencyOffsets[localId] = copyValues(storage, requiredBytes, degree, adjacencyAllocator);
        weightOffsets[localId] = copyValues(compressedWeights, weightsRequiredBytes, degree, weightsAllocator);

        array.release();
        return degree;
    }

    private synchronized long copyValues(
            byte[] targets,
            int requiredBytes,
            int degree,
            HugeAdjacencyListBuilder.Allocator allocator) {
        // sizeOf(degree) + compression bytes
        long address = allocator.allocate(4 + requiredBytes);
        int offset = allocator.offset;
        offset = writeDegree(allocator.page, offset, degree);
        System.arraycopy(targets, 0, allocator.page, offset, requiredBytes);
        allocator.offset = (offset + requiredBytes);
        return address;
    }

    int degree(int localId) {
        return (int) adjacencyOffsets[localId];
    }
}
