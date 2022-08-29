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

public abstract class RecordsBatchBuffer<Reference> implements StoreScanner.RecordConsumer<Reference> {

    public static final int DEFAULT_BUFFER_SIZE = 100_000;

    final long[] buffer;
    int length;

    RecordsBatchBuffer(int capacity) {
        this.buffer = new long[capacity];
    }

    static final class ScanState {
        private final boolean batchHasData;
        private final boolean batchConsumed;

        ScanState(boolean batchHasData, boolean batchConsumed) {
            this.batchHasData = batchHasData;
            this.batchConsumed = batchConsumed;
        }

        /**
         * True, if it is safe to advance the underlying scan to the next batch.
         */
        boolean reserveNextBatch() {
            return batchConsumed;
        }

        /**
         * True, if the underlying buffers must be flushed before consuming more records.
         */
        boolean requiresFlush() {
            return batchHasData;
        }
    }

    /**
     * Advances the underlying scan to the next batch of records and immediately consumes
     * the batch into this {@link org.neo4j.gds.core.loading.RecordsBatchBuffer}.
     * <p/>
     * There are two scenarios that can happen while consuming a batch of records from the
     * kernel. If we read in fixed-size batches, these batches usually align with the
     * buffer size and once we consumed the whole batch, the buffers can be flushed to
     * the importer tasks. In the second scenario, we read from partitioned index scans
     * which have varying sizes for partitions that might also exceed the size of the buffer.
     * In that scenario, we need to flush the buffer <b>before</b> we advance to the next
     * batch. The two scenarios are indicated by the returned {@code ScanState}.
     *
     * @param cursor           A wrapper around a {@link org.neo4j.internal.kernel.api.Cursor} that allows
     *                         us to consume the entity records using this buffer instance.
     * @param reserveNextBatch Indicates if the underlying kernel scan should advance the cursor to
     *                         the next batch. This needs to be set to false, if the {@code RecordsBatchBuffer}
     *                         has not read the complete batch yet, but had to be flushed before that.
     * @return a {@code ScanState} that indicates if the consumer needs to be flushed and
     *     if another batch can be read from the underlying scan.
     */
    public ScanState scan(StoreScanner.ScanCursor<Reference> cursor, boolean reserveNextBatch) {
        reset();

        boolean batchHasData = !reserveNextBatch || cursor.reserveBatch();
        boolean batchConsumed = !batchHasData || cursor.consumeBatch(this);

        return new ScanState(batchHasData, batchConsumed);
    }

    public int length() {
        return length;
    }

    public int capacity() {
        return buffer.length;
    }

    public boolean isFull() {
        return length >= buffer.length;
    }

    public void reset() {
        this.length = 0;
    }

    public long[] batch() {
        return buffer;
    }

}
