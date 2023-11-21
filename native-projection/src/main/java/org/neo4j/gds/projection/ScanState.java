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
package org.neo4j.gds.projection;

import org.jetbrains.annotations.Contract;

final class ScanState {
    private boolean batchConsumed;

    public static ScanState of() {
        return new ScanState(true);
    }

    private ScanState(boolean batchConsumed) {
        this.batchConsumed = batchConsumed;
    }

    /**
     * Advances the provided scan to the next batch of records and immediately consumes
     * the batch into the provided {@link org.neo4j.gds.projection.StoreScanner.RecordConsumer}.
     * <br>
     * There are two scenarios that can happen while consuming a batch of records from the
     * kernel. If we read in fixed-size batches, these batches usually align with the
     * buffer size and once we consumed the whole batch, the buffers can be flushed to
     * the importer tasks. In the second scenario, we read from partitioned index scans
     * which have varying sizes for partitions that might also exceed the size of the buffer.
     * In that scenario, we need to flush the buffer <b>before</b> we advance to the next
     * batch. The two scenarios are handled internally, as long as the {@code scan} method
     * returns {@code true}, the consumer needs to be flushed.
     *
     * @param cursor   A wrapper around a {@link org.neo4j.internal.kernel.api.Cursor} that allows
     *                 us to consume the records in batches.
     * @param consumer A consumer of records.
     * @return {@code true} if there is some data in the batch that needs to be consumed before the next call
     *     to {@code scan}.
     *     {@code false} if there is no more data. It is expected that once {@code scan} returns {@code false},
     *     it will never return {@code true} afterward.
     */
    @Contract(mutates = "this")
    public <Reference> boolean scan(
        StoreScanner.ScanCursor<Reference> cursor,
        StoreScanner.RecordConsumer<? super Reference> consumer
    ) {
        consumer.reset();

        boolean batchHasData = !this.batchConsumed || cursor.reserveBatch();
        // We proceed to the next batch, iff the current batch is
        // completely consumed. If not, we remain in the current
        // batch, flush the buffers and consume until the batch is
        // drained.
        this.batchConsumed = !batchHasData || cursor.consumeBatch(consumer);

        return batchHasData;
    }
}
