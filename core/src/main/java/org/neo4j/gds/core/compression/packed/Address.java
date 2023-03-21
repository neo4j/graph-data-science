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

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

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
