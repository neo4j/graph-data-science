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

import org.jetbrains.annotations.Nullable;
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

    public long[] @Nullable [] properties() {
        return this.properties;
    }

    public void properties(long[][] properties) {
        this.properties = properties.clone();
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
