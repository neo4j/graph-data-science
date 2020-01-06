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
package org.neo4j.internal.kernel.api.helpers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.core.loading.NodesHelper.countUndirected;

final class NodesHelperTest {

    @Test
    void shouldCountUndirectedDense() {
        StubGroupCursor groupCursor = new StubGroupCursor(
                group().withOutCount(1).withInCount(1).withLoopCount(5),
                group().withOutCount(1).withInCount(1).withLoopCount(3),
                group().withOutCount(2).withInCount(1).withLoopCount(2),
                group().withOutCount(3).withInCount(1).withLoopCount(1),
                group().withOutCount(5).withInCount(1).withLoopCount(1)
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors(groupCursor);

        int count = countUndirected(new StubNodeCursor(true), cursors);

        assertEquals(41, count);
    }

    @Test
    void shouldCountUndirectedSparse() {
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain(11)
                        .outgoing(55, 0, 1)
                        .incoming(56, 0, 1)
                        .outgoing(57, 0, 1)
                        .loop(58, 0));
        StubCursorFactory cursors = new StubCursorFactory().withRelationshipTraversalCursors(relationshipCursor);

        StubNodeCursor nodeCursor = new StubNodeCursor(false).withNode(11);
        nodeCursor.next();

        int count = countUndirected(nodeCursor, cursors);

        assertEquals(5, count);
    }

    @Test
    void shouldCountUndirectedWithTypeDense() {
        StubGroupCursor groupCursor = new StubGroupCursor(
                group(1).withOutCount(1).withInCount(1).withLoopCount(5),
                group(2).withOutCount(1).withInCount(1).withLoopCount(3)
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors(groupCursor, groupCursor);

        int countType1 = countUndirected(new StubNodeCursor(true), cursors, 1);
        int countType2 = countUndirected(new StubNodeCursor(true), cursors, 2);

        assertEquals(12, countType1);
        assertEquals(8, countType2);
    }

    @Test
    void shouldCountUndirectedWithTypeSparse() {
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain(11)
                        .outgoing(55, 0, 1)
                        .incoming(56, 0, 1)
                        .outgoing(57, 0, 1)
                        .loop(58, 1)
                        .incoming(59, 0, 2)
                        .outgoing(60, 0, 2)
                        .loop(61, 2)
                        .loop(62, 2));
        StubCursorFactory cursors = new StubCursorFactory(true)
                .withRelationshipTraversalCursors(relationshipCursor);

        StubNodeCursor nodeCursor = new StubNodeCursor(false).withNode(11);
        nodeCursor.next();

        int countType1 = countUndirected(nodeCursor, cursors, 1);
        int countType2 = countUndirected(nodeCursor, cursors, 2);

        assertEquals(5, countType1);
        assertEquals(6, countType2);
    }

    private StubGroupCursor.GroupData group() {
        return new StubGroupCursor.GroupData(0, 0, 0, 0);
    }

    private StubGroupCursor.GroupData group(int type) {
        return new StubGroupCursor.GroupData(0, 0, 0, type);
    }
}
