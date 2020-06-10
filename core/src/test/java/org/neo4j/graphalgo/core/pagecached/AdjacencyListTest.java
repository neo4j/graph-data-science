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
package org.neo4j.graphalgo.core.pagecached;

import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.concurrency.ConcurrencyControllerExtension;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

@DbmsExtension(configurationCallback = "configuration")
class AdjacencyListTest {

    @Inject
    public GraphDatabaseAPI db;

    @Inject
    FileSystemAbstraction fs;

    @Inject
    TestDirectory dir;


    @BeforeEach
    void setUp() {
        runQuery(db, "CREATE (n);");
    }

    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
//        builder.noOpSystemGraphInitializer();
        builder.addExtension(new ConcurrencyControllerExtension());
    }

    @Test
    void writeAdjacencyList() throws IOException {
        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(db, PageCache.class);
        AdjacencyListBuilder adjacencyListBuilder = AdjacencyListBuilder.newBuilder(pageCache, AllocationTracker.EMPTY);
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
    void test() throws IOException, InterruptedException {

        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(db, PageCache.class);

        File file = dir.file("gdsstore.al.1");
//        try (StoreChannel channel = fs.write(file)) {
//            ByteBuffer buf = ByteBuffers.allocate(8, EmptyMemoryTracker.INSTANCE);
//            buf.putLong(1337);
//            buf.flip();
//            channel.writeAll(buf);
//        }

        PagedFile pagedFile = pageCache.map(
            file,
            PageCache.PAGE_SIZE,
            Sets.immutable.of(StandardOpenOption.CREATE)
        );


        var st = SecureTransaction.of(db);
        st.accept((tx, ktx) -> {
            var pageCursor = Neo4jProxy.pageFileIO(
                pagedFile,
                0,
                PagedFile.PF_SHARED_WRITE_LOCK,
                ktx.pageCursorTracer()
            );
            if (!pageCursor.next()) {
                throw new IllegalStateException("Cannot open page 0");
            }

            pageCursor.putLong(42);
            pageCursor.checkAndClearCursorException();
            if (pageCursor.checkAndClearBoundsFlag()) {
                throw new IllegalStateException("out of bounds");
            }
            pageCursor.close();
        });

        pagedFile.flushAndForce();

        st.accept((tx, ktx) -> {
            var pageCursor = Neo4jProxy.pageFileIO(
                pagedFile,
                0,
                PagedFile.PF_SHARED_WRITE_LOCK,
                ktx.pageCursorTracer()
            );
            if (!pageCursor.next()) {
                throw new IllegalStateException("Cannot open page 0");
            }

            var aLong = pageCursor.getLong();
            System.out.println("aLong = " + aLong);
            pageCursor.close();
        });

        var path = pagedFile.file().toPath();
        System.out.println("pagedFile.file() = " + path);
//        pagedFile.close();

//        pageCache.flushAndForce();

//        pageCache.close();


//        Thread.sleep(1000);
        System.out.println("path = " + path);


        try (var fc = FileChannel.open(path)) {
            var buffer = ByteBuffer.allocate(8);
            fc.read(buffer);
            buffer.flip();
            var bLong = buffer.getLong();
            System.out.println("bLong = " + bLong);
        }
    }
}
