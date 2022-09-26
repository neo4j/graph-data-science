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

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.util.concurrent.atomic.AtomicLong;

public final class ArrayIdMapBuilder implements IdMapBuilder {

    private final HugeLongArray array;
    private final long capacity;
    private final AtomicLong allocationIndex;
    private final CloseableThreadLocal<BulkAdder> adders;

    public static ArrayIdMapBuilder of(long capacity) {
        HugeLongArray array = HugeLongArray.newArray(capacity);
        return new ArrayIdMapBuilder(array, capacity);
    }

    private ArrayIdMapBuilder(HugeLongArray array, final long capacity) {
        this.array = array;
        this.capacity = capacity;
        this.allocationIndex = new AtomicLong();
        this.adders = CloseableThreadLocal.withInitial(this::newBulkAdder);
    }

    @Override
    public BulkAdder allocate(int batchLength) {
        long startIndex = allocationIndex.getAndAccumulate(batchLength, this::upperAllocation);
        BulkAdder adder = adders.get();
        adder.reset(startIndex, upperAllocation(startIndex, batchLength));
        return adder;
    }

    private BulkAdder newBulkAdder() {
        return new BulkAdder(array, array.newCursor());
    }

    private long upperAllocation(long lower, long nodes) {
        return Math.min(capacity, lower + nodes);
    }

    @Override
    public IdMap build(
        LabelInformation.Builder labelInformationBuilder,
        long highestNodeId,
        int concurrency
    ) {
        adders.close();
        long nodeCount = this.size();
        var graphIds = this.array();
        return ArrayIdMapBuilderOps.build(graphIds, nodeCount, labelInformationBuilder, highestNodeId, concurrency);
    }

    public HugeLongArray array() {
        return array;
    }

    public long size() {
        return allocationIndex.get();
    }

    public static final class BulkAdder implements IdMapAllocator {
        private long[] buffer;
        private int allocationSize;
        private int offset;
        private int length;
        private final HugeLongArray array;
        private final HugeCursor<long[]> cursor;

        private BulkAdder(HugeLongArray array, HugeCursor<long[]> cursor) {
            this.array = array;
            this.cursor = cursor;
        }

        private void reset(long start, long end) {
            array.initCursor(this.cursor, start, end);
            this.buffer = null;
            this.allocationSize = (int) (end - start);
            this.offset = 0;
            this.length = 0;
        }

        public boolean nextBuffer() {
            if (!cursor.next()) {
                return false;
            }
            buffer = cursor.array;
            offset = cursor.offset;
            length = cursor.limit - cursor.offset;
            return true;
        }

        @Override
        public int allocatedSize() {
            return this.allocationSize;
        }

        @Override
        public void insert(long[] nodeIds) {
            int batchOffset = 0;
            while (nextBuffer()) {
                System.arraycopy(nodeIds, batchOffset, this.buffer, this.offset, this.length);
                batchOffset += this.length;
            }
        }
    }
}
