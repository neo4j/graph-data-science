/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.PaddedAtomicLong;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.offsetForId;

abstract class AbstractRecordBasedScanner<Reference, Record extends AbstractBaseRecord, Store extends RecordStore<Record>> implements StoreScanner<Reference> {

    private final class ScanCursor implements StoreScanner.ScanCursor<Reference> {

        // last page to contain a value of interest, inclusive, but we mostly
        // treat is as exclusive since we want to special-case the last page
        private final long lastPage;
        // end offset of the last page, exclusive (first offset to be out-of-range)
        private final int lastOffset;

        // thread-local cursor instance
        private PageCursor pageCursor;
        // thread-local record instance
        private Record record;
        private Reference recordReference;

        // the current record id -
        private long recordId;
        // the current page
        private long currentPage;
        // the last page that has already been fetched - exclusive
        private long fetchedUntilPage;
        // the current offset into the page
        private int offset;

        ScanCursor(PageCursor pageCursor, Record record, Reference reference) {
            this.lastOffset = offsetForId(maxId, pageSize, recordSize);
            this.lastPage = calculateLastPageId(maxId, recordsPerPage, lastOffset);
            this.pageCursor = pageCursor;
            this.record = record;
            this.recordReference = reference;
            this.offset = pageSize; // trigger page load as first action
            this.currentPage = -1;
        }

        @Override
        public int bulkSize() {
            return prefetchSize * recordsPerPage;
        }

        private long calculateLastPageId(long maxId, long recordsPerPage, int lastPageOffset) {
            long lastPageId = BitUtil.ceilDiv(maxId, recordsPerPage) - 1L;
            lastPageId = Math.max(lastPageId, 0L);
            if (lastPageOffset == 0) {
                lastPageId += 1;
            }
            return lastPageId;
        }

        @Override
        public boolean bulkNext(RecordConsumer<Reference> consumer) {
            try {
                return bulkNextInternal(consumer);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private boolean bulkNextInternal(RecordConsumer<Reference> consumer) throws IOException {
            if (recordId == -1L) {
                return false;
            }

            int endOffset;
            long page;
            long endPage;
            if (currentPage < lastPage) {
                preFetchPages();
            }
            if (currentPage == lastPage) {
                page = lastPage;
                endOffset = lastOffset;
                endPage = 1L + page;
            } else if (currentPage > lastPage) {
                this.recordId = -1L;
                return false;
            } else {
                page = currentPage;
                endPage = fetchedUntilPage;
                endOffset = pageSize;
            }

            int offset = this.offset;
            long recordId = page * (long) recordsPerPage;
            int recordSize = AbstractRecordBasedScanner.this.recordSize;
            PageCursor pageCursor = this.pageCursor;
            Record record = this.record;
            Reference recordReference = this.recordReference;

            while (page < endPage) {
                if (!pageCursor.next(page++)) {
                    break;
                }
                offset = 0;

                while (offset < endOffset) {
                    record.setId(recordId++); // do we need this setId command here?
                    loadAtOffset(offset);
                    offset += recordSize;
                    if (record.inUse()) {
                        consumer.offer(recordReference);
                    }
                }
            }

            currentPage = page;
            this.offset = offset;
            this.recordId = recordId;

            return true;
        }

        private void preFetchPages() throws IOException {
            PageCursor pageCursor = this.pageCursor;
            long prefetchSize = AbstractRecordBasedScanner.this.prefetchSize;
            long startPage = nextPageId.getAndAdd(prefetchSize);
            long endPage = Math.min(lastPage, startPage + prefetchSize);
            long preFetchedPage = startPage;
            while (preFetchedPage < endPage) {
                if (!pageCursor.next(preFetchedPage)) {
                    break;
                }
                ++preFetchedPage;
            }
            this.currentPage = startPage;
            this.fetchedUntilPage = preFetchedPage;
        }

        private void loadAtOffset(int offset) throws IOException {
            do {
                record.setInUse(false);
                pageCursor.setOffset(offset);
                recordFormat.read(record, pageCursor, RecordLoad.CHECK, recordSize);
            } while (pageCursor.shouldRetry());
            verifyLoad();
        }

        private void verifyLoad() {
            pageCursor.checkAndClearBoundsFlag();
        }

        @SuppressWarnings("ConstantConditions")
        @Override
        public void close() {
            if (pageCursor != null) {
                pageCursor.close();
                pageCursor = null;
                record = null;
                recordReference = null;

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
    // global pointer which block of pages need to be fetched next
    private final AtomicLong nextPageId;
    // global cursor pool to return this one to
    private final ThreadLocal<StoreScanner.ScanCursor<Reference>> cursors;

    // size in bytes of a single record - advance the offset by this much
    private final int recordSize;
    // how many records are there in a single page
    private final int recordsPerPage;

    private final long maxId;
    private final int pageSize;
    // how to read the record
    private final RecordFormat<Record> recordFormat;
    private final Store store;
    private final PagedFile pagedFile;

    AbstractRecordBasedScanner(int prefetchSize, GraphDatabaseService api) {

        var neoStores = GraphDatabaseApiProxy.neoStores(api);
        var store = store(neoStores);
        int recordSize = store.getRecordSize();
        int recordsPerPage = store.getRecordsPerPage();
        int pageSize = recordsPerPage * recordSize;

        PagedFile pagedFile = null;
        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(api, PageCache.class);
        String storeFileName = storeFileName();
        try {
            for (PagedFile pf : pageCache.listExistingMappings()) {
                if (pf.file().getName().equals(storeFileName)) {
                    pageSize = pf.pageSize();
                    recordsPerPage = pageSize / recordSize;
                    pagedFile = pf;
                    break;
                }
            }
        } catch (IOException ignored) {
        }

        this.prefetchSize = prefetchSize;
        this.nextPageId = new PaddedAtomicLong();
        this.cursors = new ThreadLocal<>();
        this.recordSize = recordSize;
        this.recordsPerPage = recordsPerPage;
        this.maxId = 1L + store.getHighestPossibleIdInUse();
        this.pageSize = pageSize;
        this.recordFormat = recordFormat(neoStores.getRecordFormats());
        this.store = store;
        this.pagedFile = pagedFile;
    }

    @Override
    public final StoreScanner.ScanCursor<Reference> getCursor(KernelTransaction transaction) {
        StoreScanner.ScanCursor<Reference> cursor = this.cursors.get();
        if (cursor == null) {
            // Don't add as we want to always call next as the first cursor action,
            // which actually does the advance and returns the correct cursor.
            // This is just to position the page cursor somewhere in the vicinity
            // of its actual next page.
            long next = nextPageId.get();

            PageCursor pageCursor;
            try {
                if (pagedFile != null) {
                    pageCursor = pagedFile.io(next, PagedFile.PF_READ_AHEAD | PagedFile.PF_SHARED_READ_LOCK);
                } else {
                    long recordId = next * (long) recordSize;
                    pageCursor = store.openPageCursorForReading(recordId);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            Record record = store.newRecord();
            Reference reference = recordReference(record, store);
            cursor = new ScanCursor(pageCursor, record, reference);
            this.cursors.set(cursor);
        }
        return cursor;
    }

    @Override
    public final long storeSize() {
        if (pagedFile != null) {
            return pagedFile.file().length();
        }
        long recordsInUse = 1L + store.getHighestPossibleIdInUse();
        long idsInPages = ((recordsInUse + (recordsPerPage - 1L)) / recordsPerPage) * recordsPerPage;
        return idsInPages * (long) recordSize;
    }

    @Override
    public void close() {
    }

    /**
     * Return the store to use.
     */
    abstract Store store(NeoStores neoStores);

    /**
     * Return the record format to use.
     */
    abstract RecordFormat<Record> recordFormat(RecordFormats formats);

    abstract Reference recordReference(Record record, Store store);

    /**
     * Return the filename of the store file that the page cache maps
     */
    abstract String storeFileName();
}
