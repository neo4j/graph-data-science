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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.SparseLongArray;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.util.concurrent.atomic.AtomicLong;

public final class InternalBitIdMappingBuilder implements InternalIdMappingBuilder<InternalBitIdMappingBuilder.BulkAdder> {

    private final SparseLongArray.Builder builder;
    private final long capacity;
    private final AtomicLong allocationIndex;
    private final CloseableThreadLocal<BulkAdder> adders;

    public static InternalBitIdMappingBuilder of(long length, AllocationTracker allocationTracker) {
        var builder = SparseLongArray.builder(length);
        return new InternalBitIdMappingBuilder(builder, length);
    }

    private InternalBitIdMappingBuilder(SparseLongArray.Builder builder, final long length) {
        this.builder = builder;
        this.capacity = length;
        this.allocationIndex = new AtomicLong();
        this.adders = CloseableThreadLocal.withInitial(this::newBulkAdder);
    }

    @Override
    public @Nullable InternalBitIdMappingBuilder.BulkAdder allocate(int batchLength) {
        return this.allocate((long) batchLength);
    }

    public BulkAdder allocate(long nodes) {
        long startIndex = allocationIndex.getAndAccumulate(nodes, this::upperAllocation);
        if (startIndex == capacity) {
            return null;
        }
        BulkAdder adder = adders.get();
        adder.reset(startIndex);
        return adder;
    }

    public SparseLongArray build() {
        adders.close();
        return builder.build();
    }

    public long capacity() {
        return capacity;
    }

    public long size() {
        return allocationIndex.get();
    }

    private BulkAdder newBulkAdder() {
        return new BulkAdder(builder);
    }

    private long upperAllocation(long lower, long nodes) {
        return Math.min(capacity, lower + nodes);
    }

    public static final class BulkAdder implements IdMappingAllocator {
        private final SparseLongArray.Builder builder;

        private long allocationIndex;

        private BulkAdder(SparseLongArray.Builder builder) {
            this.builder = builder;
        }

        private void reset(long allocationIndex) {
            this.allocationIndex = allocationIndex;
        }

        @Override
        public long startId() {
            return allocationIndex;
        }

        @Override
        public int insert(
            long[] nodeIds,
            int length,
            PropertyAllocator propertyAllocator,
            NodeImporter.PropertyReader reader,
            PropertyReference[] properties,
            long[][] labelIds
        ) {
            builder.set(allocationIndex, nodeIds, 0, length);
            return propertyAllocator.allocateProperties(reader, nodeIds, properties, labelIds, 0, length, allocationIndex);
        }
    }
}
