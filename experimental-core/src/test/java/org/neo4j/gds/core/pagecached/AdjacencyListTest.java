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
package org.neo4j.gds.core.pagecached;import org.apache.lucene.util.LongsRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.VarLongEncoding;
import org.neo4j.io.pagecache.PageCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdjacencyListTest extends BaseTest {

    @Test
    void writeAdjacencyList() throws IOException {
        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(db, PageCache.class);
        AdjacencyListBuilder adjacencyListBuilder = AdjacencyListBuilder.newBuilder(pageCache);
        AdjacencyListBuilder.Allocator allocator = adjacencyListBuilder.newAllocator();

        allocator.prepare();
        int length = 20;
        long offset = allocator.allocate(length);
        allocator.putInt(2);
        allocator.putLong(1);
        allocator.putLong(42);

        allocator.close();
        AdjacencyList adjacencyList = adjacencyListBuilder.build();
        AdjacencyList.Cursor cursor = adjacencyList.cursor(offset);
        assertEquals(2, cursor.length());
        assertEquals(1, cursor.nextLong());
        assertEquals(42, cursor.nextLong());
    }

    @Test
    void writeBufferIntoAdjacencyList() throws IOException {
        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(db, PageCache.class);
        AdjacencyListBuilder adjacencyListBuilder = AdjacencyListBuilder.newBuilder(pageCache);
        AdjacencyListBuilder.Allocator allocator = adjacencyListBuilder.newAllocator();

        ByteBuffer buffer = ByteBuffer.allocate(24);
        buffer.putLong(2);
        buffer.putLong(1);
        buffer.putLong(42);

        allocator.prepare();
        long offset = allocator.insert(buffer.array(), buffer.arrayOffset() + 4, 20);

        allocator.close();
        AdjacencyList adjacencyList = adjacencyListBuilder.build();
        AdjacencyList.Cursor cursor = adjacencyList.cursor(offset);
        assertEquals(2, cursor.length());
        assertEquals(1, cursor.nextLong());
        assertEquals(42, cursor.nextLong());
    }

    @Test
    void writeLargeBufferIntoAdjacencyList() throws IOException {
        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(db, PageCache.class);
        AdjacencyListBuilder adjacencyListBuilder = AdjacencyListBuilder.newBuilder(pageCache);
        AdjacencyListBuilder.Allocator allocator = adjacencyListBuilder.newAllocator();

        ByteBuffer buffer = ByteBuffer.allocate(1337 * 8 + 4);
        buffer.putInt(1337);
        for (int i = 0; i < 1337; i++) {
            buffer.putLong(i);
        }

        allocator.prepare();
        long offset = allocator.insert(buffer.array(), buffer.arrayOffset(), 1337 * 8 + 4);

        allocator.close();
        AdjacencyList adjacencyList = adjacencyListBuilder.build();
        AdjacencyList.Cursor cursor = adjacencyList.cursor(offset);
        assertEquals(1337, cursor.length());
        for (int i = 0; i < 1337; i++) {
            assertEquals(i, cursor.nextLong());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {42, 13370})
    void writeCompressedAdjacencyValues(int numberOfValues) throws IOException {
        var random = new Random();
        var uncompressedAdjacencyList = random.longs(0, 1337 * numberOfValues).limit(numberOfValues).toArray();

        var uncompressedValues = new LongsRef(0);
        uncompressedValues.longs = uncompressedAdjacencyList.clone();
        uncompressedValues.length = uncompressedAdjacencyList.length;

        var degree = AdjacencyCompression.applyDeltaEncoding(uncompressedValues, Aggregation.NONE);
        var compressed = new byte[Math.multiplyExact(degree, 10) + 4];
        AdjacencyCompression.writeBEInt(compressed, 0, degree);
        var requiredBytes = VarLongEncoding.encodeVLongs(
            uncompressedValues.longs,
            uncompressedValues.length,
            compressed,
            4
        );

        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(db, PageCache.class);
        AdjacencyListBuilder adjacencyListBuilder = AdjacencyListBuilder.newBuilder(pageCache);
        AdjacencyListBuilder.Allocator allocator = adjacencyListBuilder.newAllocator();

        allocator.prepare();
        long offset = allocator.insert(compressed, 0, requiredBytes);
        allocator.close();


        AdjacencyList adjacencyList = adjacencyListBuilder.build();
        AdjacencyList.DecompressingCursor cursor = adjacencyList.decompressingCursor(offset);
        assertEquals(numberOfValues, cursor.length());

        Arrays.sort(uncompressedAdjacencyList);
        for (int i = 0; i < numberOfValues; i++) {
            assertEquals(uncompressedAdjacencyList[i], cursor.nextVLong());
        }
    }
}
