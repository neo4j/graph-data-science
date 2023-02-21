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
package org.neo4j.gds.core.compression.packed;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Represents a slice of compressed memory that lives off-heap.
 */
public final class Compressed implements AutoCloseable {

    private static final Cleaner CLEANER = Cleaner.create();

    public static final Compressed EMPTY = new Compressed();

    private final Address address;
    private final Cleaner.Cleanable cleanable;

    // number of compressed values
    private final int length;
    // header of compression blocks
    private final byte[] header;

    private long[][] properties;

    private Compressed() {
        this.address = Address.EMPTY;
        this.header = new byte[0];
        this.length = 0;
        this.cleanable = () -> {};
        this.properties = null;
    }

    public Compressed(long address, long bytes, byte[] header, int length) {
        this.address = Address.createAddress(address, bytes);
        this.cleanable = CLEANER.register(this, this.address);
        this.header = header;
        this.length = length;
        this.properties = null;
    }

    @Override
    public String toString() {
        return "Compressed{address=" + this.address.address() + ", bytes=" + this.address.bytes() + ", blocks=" + header.length + '}';
    }

    public void properties(long[][] properties) {
        this.properties = properties;
    }

    /**
     * Free the underlying memory.
     *
     * @see Compressed#free()
     */
    @Override
    public void close() {
        this.free();
    }

    /**
     * Free the underlying memory.
     * <p>
     * The memory must not have been already freed.
     *
     * @throws IllegalStateException if the memory has already been freed.
     */
    public void free() {
        // make sure that the address is valid
        // We already prevent double-free because the cleanable has
        // exactly-once semantics, but additional calls to clean would just
        // do nothing, and we want to throw an exception instead.
        this.address.address();
        this.cleanable.clean();
    }

    /**
     * @return number of bytes to represent the data in compressed format.
     */
    public long bytesUsed() {
        return this.address.bytes() + this.header.length;
    }

    /**
     * @return a valid (non zero) address.
     * @throws IllegalStateException if the address is NULL.
     */
    long address() {
        return this.address.address();
    }

    /**
     * @return the headers of compression blocks
     */
    byte[] header() {
        return this.header;
    }

    /**
     * @return number of compressed values
     */
    int length() {
        return this.length;
    }
}


/**
 * An address to some off-heap memory.
 * <p>
 * Separated address state to register with the {@link java.lang.ref.Cleaner}.
 * This is an auxiliary class to prevent accidental access to private members.
 */
final class Address implements Runnable {

    static Address EMPTY = new Address(0, 0);

    private static final AtomicLongFieldUpdater<Address> ADDRESS = AtomicLongFieldUpdater.newUpdater(
        Address.class,
        "address"
    );

    private volatile long address;
    private final long bytes;

    private Address(long address, long bytes) {
        this.address = address;
        this.bytes = bytes;
    }

    static Address createAddress(long address, long bytes) {
        requirePointerIsValid(address);
        return new Address(address, bytes);
    }

    @Override
    public void run() {
        long address = ADDRESS.getAndSet(this, 0L);
        requirePointerIsValid(address);
        UnsafeUtil.free(address, bytes, EmptyMemoryTracker.INSTANCE);
    }

    /**
     * @return a valid (non zero) address.
     * @throws IllegalStateException if the address is NULL.
     */
    long address() {
        var address = ADDRESS.get(this);
        requirePointerIsValid(address);
        return address;
    }

    long bytes() {
        return this.bytes;
    }

    private static void requirePointerIsValid(long address) {
        if (address == 0) {
            throw new IllegalStateException("This compressed memory has already been freed.");
        }
    }
}

final class DecompressingCursor implements AdjacencyCursor {

    private final HugeObjectArray<Compressed> adjacencies;

    private final BlockDecompressor decompressingReader;

    private int maxTargets;
    private int currentPosition;

    DecompressingCursor(HugeObjectArray<Compressed> adjacencies, int flags) {
        this.adjacencies = adjacencies;
        this.decompressingReader = new BlockDecompressor(flags);

    }

    void init(long node) {
        init(node, 42);
    }

    @Override
    public void init(long node, int ignore) {
        var compressed = this.adjacencies.getOrDefault(node, Compressed.EMPTY);
        this.maxTargets = compressed.length();
        this.currentPosition = 0;
        this.decompressingReader.reset(compressed);
    }

    @Override
    public int size() {
        return this.maxTargets;
    }

    @Override
    public int remaining() {
        return this.maxTargets - this.currentPosition;
    }

    @Override
    public boolean hasNextVLong() {
        return currentPosition < maxTargets;
    }

    @Override
    public long nextVLong() {
        this.currentPosition++;
        return decompressingReader.next();
    }

    @Override
    public long peekVLong() {
        return decompressingReader.peek();
    }

    @Override
    public long skipUntil(long targetId) {
        long next;
        while (hasNextVLong()) {
            if ((next = nextVLong()) > targetId) {
                return next;
            }
        }
        return AdjacencyCursor.NOT_FOUND;
    }

    @Override
    public long advance(long targetId) {
        long next;
        while (hasNextVLong()) {
            if ((next = nextVLong()) >= targetId) {
                return next;
            }
        }
        return AdjacencyCursor.NOT_FOUND;
    }

    @Override
    public long advanceBy(int n) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination) {
        throw new UnsupportedOperationException("not yet implemented");
    }
}

final class BlockDecompressor {

    private static final int BLOCK_SIZE = AdjacencyPacking.BLOCK_SIZE;

    private final boolean isDeltaCompressed;

    // Compressed
    private long ptr;
    private byte[] header;
    private int length;

    // Decompression state
    private final long[] block;

    private int idxInBlock;
    private int blockId;
    private int blockOffset;
    private long lastValue;


    BlockDecompressor(int flags) {
        this.isDeltaCompressed = (flags & AdjacencyPacker.DELTA) == AdjacencyPacker.DELTA;
        this.block = new long[BLOCK_SIZE];
    }

    void reset(Compressed compressed) {
        this.ptr = compressed.address();
        this.header = compressed.header();
        this.length = compressed.length();
        this.idxInBlock = 0;
        this.blockId = 0;
        this.blockOffset = 0;
        this.lastValue = 0;

        this.decompressBlock();
    }

    long next() {
        if (this.idxInBlock == BLOCK_SIZE) {
            decompressBlock();
        }
        return block[this.idxInBlock++];
    }

    long peek() {
        if (this.idxInBlock == BLOCK_SIZE) {
            decompressBlock();
        }
        return block[this.idxInBlock];
    }

    private void decompressBlock() {
        if (this.blockId < this.header.length) {
            // block unpacking
            byte blockHeader = this.header[blockId];
            this.ptr = AdjacencyUnpacking.unpack(blockHeader, this.block, 0, this.ptr);
            if (this.isDeltaCompressed) {
                long value = this.lastValue;
                for (int i = 0; i < AdjacencyPacking.BLOCK_SIZE; i++) {
                    value = this.block[i] += value;
                }
                this.lastValue = value;
            }
            this.blockOffset += BLOCK_SIZE;
            this.blockId++;
        } else {
            // tail decompression
            int tailLength = this.length - this.blockOffset;
            if (this.isDeltaCompressed) {
                AdjacencyCompression.decompressAndPrefixSum(tailLength, this.lastValue, this.ptr, this.block, 0);
            } else {
                AdjacencyCompression.decompress(tailLength, this.ptr, this.block, 0);
            }
        }

        this.idxInBlock = 0;
    }
}
