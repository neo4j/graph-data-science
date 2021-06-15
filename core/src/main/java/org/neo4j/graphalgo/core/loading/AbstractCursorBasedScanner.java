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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.core.TransactionContext;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.kernel.api.KernelTransaction;

abstract class AbstractCursorBasedScanner<Reference, EntityCursor extends Cursor, Attachment>
    implements StoreScanner<Reference> {

    private final class ScanCursor implements StoreScanner.ScanCursor<Reference> {

        private final StoreScan<EntityCursor> scan;
        private final boolean patchForLabelScanAlignment;

        private EntityCursor cursor;

        private Reference cursorReference;

        ScanCursor(
            EntityCursor cursor,
            Reference reference,
            StoreScan<EntityCursor> entityCursorScan,
            boolean patchForLabelScanAlignment
        ) {
            this.cursor = cursor;
            this.cursorReference = reference;
            this.scan = entityCursorScan;
            this.patchForLabelScanAlignment = patchForLabelScanAlignment;
        }

        @Override
        public int bulkSize() {
            // We want to scan about 100 pages per bulk, so start with that value
            var bulkSize = prefetchSize * recordsPerPage;

            // We need to make sure that we scan aligned to the super block size, as we are not
            // allowed to write into the same block multiple times.
            bulkSize = SparseLongArray.toValidBatchSize(bulkSize);

            // The label scan cursor on Neo4j <= 4.1 has a bug where it would add 64 to the bulks size
            // even if the value is already divisible by 64. (#6156)
            // We need to subtract 64 on those cases, to get a final bulk size of what we really want.
            if (patchForLabelScanAlignment) {
                bulkSize = Math.max(0, bulkSize - 64);
            }

            return bulkSize;
        }

        @Override
        public int bufferSize() {
            return bulkSize() + (patchForLabelScanAlignment ? Long.SIZE : 0);
        }

        @Override
        public boolean bulkNext(RecordConsumer<Reference> consumer) {
            boolean hasNextBatch = scan.scanBatch(cursor, bulkSize());

            if (!hasNextBatch) {
                return false;
            }

            while (cursor.next()) {
                consumer.offer(cursorReference);
            }

            return true;
        }

        @Override
        public void close() {
            if (cursor != null) {
                closeCursorReference(cursorReference);
                cursorReference = null;
                cursor.close();
                cursor = null;

                final StoreScanner.ScanCursor<Reference> localCursor = cursors.get();
                // sanity check, should always be called from the same thread
                if (localCursor == this) {
                    cursors.remove();
                }
            }
        }
    }

    // fetch this many pages at once
    private final int prefetchSize;
    // how many records are there in a single page
    private final int recordsPerPage;

    private final TransactionContext.SecureTransaction transaction;
    // global cursor pool to return this one to
    private final ThreadLocal<StoreScanner.ScanCursor<Reference>> cursors;

    private final StoreScan<EntityCursor> entityCursorScan;

    AbstractCursorBasedScanner(int prefetchSize, TransactionContext transactionContext, Attachment attachment) {
        this.transaction = transactionContext.fork();
        this.prefetchSize = prefetchSize;
        this.entityCursorScan = entityCursorScan(this.transaction.kernelTransaction(), attachment);
        this.cursors = new ThreadLocal<>();
        this.recordsPerPage = recordsPerPage();
    }

    @Override
    public void close() {
        transaction.close();
    }

    @Override
    public final StoreScanner.ScanCursor<Reference> getCursor(KernelTransaction transaction) {
        StoreScanner.ScanCursor<Reference> scanCursor = this.cursors.get();

        if (scanCursor == null) {
            EntityCursor entityCursor = entityCursor(transaction);
            Reference reference = cursorReference(transaction, entityCursor);
            scanCursor = new ScanCursor(
                entityCursor,
                reference,
                entityCursorScan,
                needsPatchingForLabelScanAlignment()
            );
            this.cursors.set(scanCursor);
        }

        return scanCursor;
    }

    abstract int recordsPerPage();

    abstract EntityCursor entityCursor(KernelTransaction transaction);

    abstract StoreScan<EntityCursor> entityCursorScan(KernelTransaction transaction, Attachment attachment);

    abstract Reference cursorReference(KernelTransaction transaction, EntityCursor cursor);

    /**
     * Close Neo4j cursors that have been allocated in {@link #cursorReference(org.neo4j.kernel.api.KernelTransaction, org.neo4j.internal.kernel.api.Cursor)}.
     * Cursors that were allocated in {@link #entityCursor(org.neo4j.kernel.api.KernelTransaction)} are closed automatically.
     */
    abstract void closeCursorReference(Reference reference);

    boolean needsPatchingForLabelScanAlignment() {
        return false;
    }
}
