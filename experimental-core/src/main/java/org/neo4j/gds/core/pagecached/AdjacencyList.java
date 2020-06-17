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
package org.neo4j.gds.core.pagecached;

import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.loading.MutableIntValue;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class AdjacencyList {

    private final PagedFile pagedFile;

    public AdjacencyList(PagedFile pagedFile) {
        this.pagedFile = pagedFile;
    }

    int getDegree(long index) throws IOException {
        PageCursor pageCursor = Neo4jProxy.pageFileIO(
            pagedFile,
            0,
            PagedFile.PF_SHARED_READ_LOCK,
            PageCursorTracer.NULL
        );
        int indexInPage = (int) (index % PageCache.PAGE_SIZE);
        pageCursor.next(index / PageCache.PAGE_SIZE);
        int degree = pageCursor.getInt(indexInPage);
        pageCursor.close();
        return degree;
    }

    public long release() {
        pagedFile.close();
        return 0L;
    }

    // Cursors

    Cursor cursor(long offset) throws IOException {
        PageCursor pageCursor = Neo4jProxy.pageFileIO(
            pagedFile,
            0,
            PagedFile.PF_SHARED_READ_LOCK,
            PageCursorTracer.NULL
        );
        return new Cursor(pageCursor).init(offset, pagedFile.pageSize());
    }

    /**
     * Returns a new, uninitialized delta cursor. Call {@link DecompressingCursor#init(long, int)}.
     */
    DecompressingCursor rawDecompressingCursor() throws IOException {
        PageCursor pageCursor = Neo4jProxy.pageFileIO(
            pagedFile,
            0,
            PagedFile.PF_SHARED_READ_LOCK,
            PageCursorTracer.NULL
        );
        return new DecompressingCursor(pageCursor);
    }

    /**
     * Get a new cursor initialised on the given offset
     */
    DecompressingCursor decompressingCursor(long offset) throws IOException {
        return rawDecompressingCursor().init(offset, pagedFile.pageSize());
    }

    /**
     * Initialise the given cursor with the given offset
     */
    DecompressingCursor decompressingCursor(DecompressingCursor reuse, long offset) throws IOException {
        return reuse.init(offset, pagedFile.pageSize());
    }

    // TODO close cursor
    public static final class Cursor extends MutableIntValue {

        static final Cursor EMPTY = new Cursor(null);

        // TODO: release when closing cursors
        private final PageCursor pageCursor;

        private int degree;
        private int offset;
        private int limit;

        private Cursor(PageCursor pageCursor) {
            this.pageCursor = pageCursor;
        }

        public int length() {
            return degree;
        }

        /**
         * Return true iff there is at least one more target to decode.
         */
        boolean hasNextLong() {
            return offset < limit;
        }

        /**
         * Read the next target id.
         * It is undefined behavior if this is called after {@link #hasNextLong()} returns {@code false}.
         */
        long nextLong() throws IOException {
            if (pageCursor.getCurrentPageSize() - offset >= Long.BYTES) {
                long value = pageCursor.getLong(offset);
                offset += Long.BYTES;
                return value;
            }

            byte[] partialLong = new byte[Long.BYTES];
            int bytesOnThisPage = pageCursor.getCurrentPageSize() - offset;
            pageCursor.setOffset(offset);
            pageCursor.getBytes(partialLong, 0, bytesOnThisPage);

            return getLong(partialLong, bytesOnThisPage);
        }

        Cursor init(long offset, int pageSize) throws IOException {
            this.offset = (int) (offset % pageSize);
            pageCursor.next(offset / pageSize);
            this.degree = pageCursor.getInt(this.offset);
            this.offset += Integer.BYTES;
            this.limit = this.offset + degree * Long.BYTES;
            return this;
        }

        private long getLong(byte[] longBytes, int alreadyRead) throws IOException {
            if (!pageCursor.next()) {
                return -1;
            }

            pageCursor.getBytes(longBytes, alreadyRead, Long.BYTES - alreadyRead);
            this.offset = pageCursor.getOffset();
            return ByteBuffer.wrap(longBytes).order(ByteOrder.BIG_ENDIAN).getLong();
        }
    }

    public static final class DecompressingCursor extends MutableIntValue implements AutoCloseable {

        // TODO: free
        private final AdjacencyDecompressingReader decompress;

        private int maxTargets;
        private int currentTarget;
        private final PageCursor pageCursor;

        private DecompressingCursor(PageCursor pageCursor) {
            this.pageCursor = pageCursor;
            this.decompress = new AdjacencyDecompressingReader();
        }

        /**
         * Return how many targets can be decoded in total. This is equivalent to the degree.
         */
        public int length() {
            return maxTargets;
        }

        /**
         * Return true iff there is at least one more target to decode.
         */
        boolean hasNextVLong() {
            return currentTarget < maxTargets;
        }

        /**
         * Read and decode the next target id.
         * It is undefined behavior if this is called after {@link #hasNextVLong()} returns {@code false}.
         */
        long nextVLong() throws IOException {
            int current = currentTarget++;
            int remaining = maxTargets - current;
            return decompress.next(remaining);
        }

        @Override
        public void close() {
            pageCursor.close();
        }

        DecompressingCursor init(long fromIndex, int pageSize) throws IOException {
            pageCursor.next(fromIndex / pageSize);
            maxTargets = decompress.reset(pageCursor, (int) (fromIndex % pageSize));
            currentTarget = 0;
            return this;
        }
    }
}
