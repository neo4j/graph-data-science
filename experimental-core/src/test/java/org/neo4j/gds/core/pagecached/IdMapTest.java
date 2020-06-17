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
package org.neo4j.gds.core.pagecached;import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.io.pagecache.PageCache;

import java.io.IOException;
import java.util.Map;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class IdMapTest extends BaseTest {

    @Test
    void testIdMap() throws IOException {
        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(db, PageCache.class);

        long nodeCount = 6;
        var longArrayBuilder = HugeLongArrayBuilder.of(nodeCount, pageCache);
        var nodeAdder = longArrayBuilder.allocate(nodeCount);
        assertNotNull(nodeAdder);

        var nodeIds = LongStream.range(42, nodeCount + 42).toArray();
        nodeAdder.insert(nodeIds, 0, (int) nodeCount);
        nodeAdder.close();

        HugeSparseLongArray.Builder nodeMappingBuilder = HugeSparseLongArray.Builder.create(
            pageCache,
            nodeCount == 0 ? 1 : nodeCount
        );

        var idMap = IdMapBuilder.build(
            longArrayBuilder,
            nodeMappingBuilder,
            Map.of(),
            1
        );

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            var originalNodeId = idMap.toOriginalNodeId(nodeId);
            assertEquals(nodeId + 42, originalNodeId);

            var mappedNodeId = idMap.toMappedNodeId(originalNodeId);
            assertEquals(nodeId, mappedNodeId);
        }

        idMap.close();
    }
}
