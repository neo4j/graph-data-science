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
import org.neo4j.gds.utils.CloseableThreadLocal;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.concurrent.atomic.AtomicLong;

public final class InternalHugeIdMappingBuilder implements InternalIdMappingBuilder<InternalHugeIdMappingBuilder.BulkAdder> {

    private final HugeLongArray array;
    private final long capacity;
    private final AtomicLong allocationIndex;
    private final CloseableThreadLocal<BulkAdder> adders;

    public static InternalHugeIdMappingBuilder of(long length, AllocationTracker tracker) {
        HugeLongArray array = HugeLongArray.newArray(length, tracker);
        return new InternalHugeIdMappingBuilder(array, length);
    }

    private InternalHugeIdMappingBuilder(HugeLongArray array, final long length) {
        this.array = array;
        this.capacity = length;
        this.allocationIndex = new AtomicLong();
        this.adders = CloseableThreadLocal.withInitial(this::newBulkAdder);
    }

    @Override
    public @Nullable InternalHugeIdMappingBuilder.BulkAdder allocate(int batchLength) {
        return this.allocate((long) batchLength);
    }

    private BulkAdder newBulkAdder() {
        return new BulkAdder(array, array.newCursor());
    }

    public BulkAdder allocate(final long nodes) {
        long startIndex = allocationIndex.getAndAccumulate(nodes, this::upperAllocation);
        if (startIndex == capacity) {
            return null;
        }
        BulkAdder adder = adders.get();
        adder.reset(startIndex, upperAllocation(startIndex, nodes));
        return adder;
    }

    private long upperAllocation(long lower, long nodes) {
        return Math.min(capacity, lower + nodes);
    }

    public HugeLongArray build() {
        adders.close();
        return array;
    }

    public long capacity() {
        return capacity;
    }

    public long size() {
        return allocationIndex.get();
    }

    public static final class BulkAdder implements IdMappingAllocator {
        public long[] buffer;
        public int offset;
        public int length;
        public long start;
        private final HugeLongArray array;
        private final HugeCursor<long[]> cursor;

        private BulkAdder(
                HugeLongArray array,
                HugeCursor<long[]> cursor) {
            this.array = array;
            this.cursor = cursor;
        }

        private void reset(long start, long end) {
            array.initCursor(this.cursor, start, end);
            this.start = start;
            buffer = null;
            offset = 0;
            length = 0;
        }

        public boolean nextBuffer() {
            if (!cursor.next()) {
                return false;
            }
            start += length;
            buffer = cursor.array;
            offset = cursor.offset;
            length = cursor.limit - cursor.offset;
            return true;
        }

        @Override
        public long startId() {
            return start;
        }

        @Override
        public int insert(
            long[] nodeIds,
            int length,
            PropertyAllocator propertyAllocator,
            NodeImporter.PropertyReader reader,
            long[] properties,
            long[][] labelIds
        ) {
            int importedProperties = 0;
            int batchOffset = 0;
            while (nextBuffer()) {
                System.arraycopy(nodeIds, batchOffset, this.buffer, this.offset, this.length);
                importedProperties += propertyAllocator.allocateProperties(reader, nodeIds, properties, labelIds, batchOffset, this.length, this.start);
                batchOffset += this.length;
            }
            return importedProperties;
        }
    }
}
