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
package org.neo4j.gds.core.loading;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public final class Compressed {
    private static final AtomicLongFieldUpdater<Compressed> ADDRESS = AtomicLongFieldUpdater.newUpdater(
        Compressed.class,
        "address"
    );

    private volatile long address;
    private final long bytes;
    private final byte[] blocks;

    public Compressed(long address, long bytes, byte[] blocks) {
        requirePointerIsValid(address);
        ADDRESS.set(this, address);
        this.bytes = bytes;
        this.blocks = blocks;
    }

    public long bytesUsed() {
        return bytes + blocks.length;
    }

    public void free() {
        long address = ADDRESS.getAndSet(this, 0L);
        requirePointerIsValid(address);
        UnsafeUtil.free(address, bytes, EmptyMemoryTracker.INSTANCE);
    }

    @Override
    public String toString() {
        return "Compressed{address=" + ADDRESS.get(this) + ", bytes=" + bytes + ", blocks=" + blocks.length + '}';
    }

    long address() {
        var address = ADDRESS.get(this);
        requirePointerIsValid(address);
        return address;
    }

    byte[] blocks() {
        return blocks;
    }

    private static void requirePointerIsValid(long address) {
        if (address == 0) {
            throw new IllegalStateException("This compressed memory has already been freed.");
        }
    }
}
