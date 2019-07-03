/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.paged;

import java.util.concurrent.atomic.AtomicLong;

abstract class HugeArrayBuilder<Array, Huge extends HugeArray<Array, ?, Huge>> {

    private final Huge array;
    private final long length;
    private final AtomicLong allocationIndex;
    private final ThreadLocal<BulkAdder<Array>> adders;

    HugeArrayBuilder(Huge array, final long length) {
        this.array = array;
        this.length = length;
        this.allocationIndex = new AtomicLong();
        this.adders = ThreadLocal.withInitial(this::newBulkAdder);
    }

    private BulkAdder<Array> newBulkAdder() {
        return new BulkAdder<>(array, array.newCursor());
    }

    public final BulkAdder<Array> allocate(final long nodes) {
        long startIndex = allocationIndex.getAndAccumulate(nodes, this::upperAllocation);
        if (startIndex == length) {
            return null;
        }
        BulkAdder<Array> adder = adders.get();
        adder.reset(startIndex, upperAllocation(startIndex, nodes));
        return adder;
    }

    private long upperAllocation(long lower, long nodes) {
        return Math.min(length, lower + nodes);
    }

    public final Huge build() {
        return array;
    }

    public final long size() {
        return allocationIndex.get();
    }

    public static final class BulkAdder<Array> {
        public Array buffer;
        public int offset;
        public int length;
        public long start;
        private final HugeArray<Array, ?, ?> array;
        private final HugeCursor<Array> cursor;

        private BulkAdder(
                HugeArray<Array, ?, ?> array,
                HugeCursor<Array> cursor) {
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
            buffer = cursor.array;
            offset = cursor.offset;
            length = cursor.limit - cursor.offset;
            return true;
        }
    }
}
