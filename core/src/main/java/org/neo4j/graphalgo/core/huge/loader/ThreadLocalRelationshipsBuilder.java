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
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, weights);
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
                .asLongBuffer()
                .put(weights, 0, degree);
        weightsAllocator.offset = (offset + requiredBytes);
        return address;
    }

    int degree(int localId) {
        return (int) adjacencyOffsets[localId];
    }
}
