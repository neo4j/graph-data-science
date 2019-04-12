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

    public final BulkAdder allocate(final long nodes) {
        long startIndex = allocationIndex.getAndAccumulate(nodes, this::upperAllocation);
        if (startIndex == length) {
            return null;
        }
        BulkAdder adder = adders.get();
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
            array.cursor(this.cursor, start, end);
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
