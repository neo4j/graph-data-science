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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.io.pagecache.PageCache;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdjacencyOffsetsTest extends BaseTest {

    @Test
    void get() throws IOException {
        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(db, PageCache.class);
        var offsets = AdjacencyOffsets.of(pageCache, new long[][]{new long[]{0, 1, 2, 3}});
        long[] values = new long[]{
            offsets.get(0),
            offsets.get(1),
            offsets.get(2),
            offsets.get(3)
        };
        offsets.release();
        assertEquals(0, values[0]);
        assertEquals(1, values[1]);
        assertEquals(2, values[2]);
        assertEquals(3, values[3]);
    }
}
