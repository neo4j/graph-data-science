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

import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;

import static org.neo4j.gds.core.pagecached.VarLongDecoding.decodeDeltaVLongs;

final class AdjacencyDecompressingReader {

    private static final int CHUNK_SIZE = 64;

    private final long[] block;
    private int pos;
    private PageCursor cursor;
    private int offset;

    AdjacencyDecompressingReader() {
        this.block = new long[CHUNK_SIZE];
    }

    int reset(PageCursor pageCursor, int offset) throws IOException {
        this.cursor = pageCursor;
        int numAdjacencies = pageCursor.getInt(offset); // offset should not be 0
        this.offset = decodeDeltaVLongs(
            0L,
            pageCursor,
            Integer.BYTES + offset,
            Math.min(numAdjacencies, CHUNK_SIZE),
            block
        );
        pos = 0;
        return numAdjacencies;
    }

    long next(int remaining) throws IOException {
        int pos = this.pos++;
        if (pos < CHUNK_SIZE) {
            return block[pos];
        }
        return readNextBlock(remaining);
    }

    private long readNextBlock(int remaining) throws IOException {
        pos = 1;
        offset = decodeDeltaVLongs(block[CHUNK_SIZE - 1], cursor, offset, Math.min(remaining, CHUNK_SIZE), block);
        return block[0];
    }
}
