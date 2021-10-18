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
import org.neo4j.gds.core.utils.paged.SparseLongArray;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.util.concurrent.atomic.AtomicLong;

public final class InternalSequentialBitIdMappingBuilder implements InternalIdMappingBuilder<InternalSequentialBitIdMappingBuilder.BulkAdder> {

    private final SparseLongArray.SequentialBuilder builder;
    private final long capacity;
    private final AtomicLong allocationIndex;
    private final CloseableThreadLocal<BulkAdder> adders;

    public static InternalSequentialBitIdMappingBuilder of(long length) {
        var builder = SparseLongArray.sequentialBuilder(length);
        return new InternalSequentialBitIdMappingBuilder(builder, length);
    }

    private InternalSequentialBitIdMappingBuilder(SparseLongArray.SequentialBuilder builder, final long length) {
        this.builder = builder;
        this.capacity = length;
        this.allocationIndex = new AtomicLong();
        this.adders = CloseableThreadLocal.withInitial(this::newBulkAdder);
    }

    @Override
    public @Nullable BulkAdder allocate(int batchLength) {
        long startIndex = allocationIndex.getAndAccumulate(batchLength, this::upperAllocation);
        if (startIndex == capacity) {
            return null;
        }
        var bulkAdder = adders.get();
        bulkAdder.setAllocationSize((int) (upperAllocation(startIndex, batchLength) - startIndex));
        return bulkAdder;
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
        private final SparseLongArray.SequentialBuilder builder;
        private int allocationSize;

        private BulkAdder(SparseLongArray.SequentialBuilder builder) {
            this.builder = builder;
        }

        @Override
        public long startId() {
            return -1;
        }

        @Override
        public int allocatedSize() {
            return this.allocationSize;
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
            builder.set(nodeIds, 0, length);
            return propertyAllocator.allocateProperties(reader, nodeIds, properties, labelIds, 0, length, -1);
        }

        private void setAllocationSize(int allocationSize) {
            this.allocationSize = allocationSize;
        }
    }
}
