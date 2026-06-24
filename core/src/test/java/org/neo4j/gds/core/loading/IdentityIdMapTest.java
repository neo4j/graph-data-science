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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.nodes.ComposedIdMap;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdentityIdMapTest {

    @Test
    void nodeCount() {
        var nodeCount = 10;
        var idMap = create(nodeCount);
        assertEquals(nodeCount, idMap.nodeCount());
    }

    @Test
    void toId() {
        var nodeCount = 10;
        var idMap = create(nodeCount);
        for (long i = 0; i < nodeCount; i++) {
            assertEquals(i, idMap.toOriginalNodeId(i));
            assertEquals(i, idMap.toRootNodeId(i));
            assertEquals(i, idMap.toMappedNodeId(i));
        }
    }

    @Test
    void highestOriginalId() {
        var nodeCount = 10;
        var idMap = create(nodeCount);
        assertEquals(nodeCount - 1, idMap.highestOriginalId());
    }

    @Test
    void containsOriginalId() {
        var nodeCount = 10;
        var idMap = create(nodeCount);
        for (long i = 0; i < nodeCount; i++) {
            assertTrue(idMap.containsOriginalId(i));
        }
        assertFalse(idMap.containsOriginalId(nodeCount));
    }

    @Test
    void builder() {
        var nodeCount = 10;
        var nodeIds = LongStream.range(0, nodeCount).toArray();
        var builder = new IdentityIdMap.Builder();

        builder.allocate(nodeIds.length).insert(nodeIds);

        var idMap = builder.build(LabelInformationBuilders.allNodes(), nodeCount - 1, new Concurrency(1));

        assertThat(idMap.nodeCount()).isEqualTo(nodeCount);

        for (var nodeId : nodeIds) {
            assertThat(idMap.toOriginalNodeId(nodeId)).isEqualTo(nodeId);
        }
    }

    private static ComposedIdMap create(int nodeCount) {
        return ComposedIdMap.of(
            new IdentityIdMap(nodeCount),
            LabelInformationBuilders.allNodes().build(nodeCount, id -> id)
        );
    }
}
