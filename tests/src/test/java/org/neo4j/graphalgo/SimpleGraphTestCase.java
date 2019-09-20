/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;

import java.util.function.LongPredicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * @author mknobloch
 */
@ExtendWith(MockitoExtension.class)
public abstract class SimpleGraphTestCase extends Neo4jTestCase {

    protected static Graph graph;

    protected static long v0, v1, v2;

    @Mock
    private RelationshipConsumer relationConsumer;

    @Mock
    private LongPredicate nodeConsumer;

    @Test
    void testForEachNode() {
        when(nodeConsumer.test(anyLong())).thenReturn(true);
        graph.forEachNode(nodeConsumer);
        verify(nodeConsumer, times(3)).test(anyLong());
        verify(nodeConsumer, times(1)).test(eq(v0));
        verify(nodeConsumer, times(1)).test(eq(v1));
        verify(nodeConsumer, times(1)).test(eq(v2));
    }

    @Test
    void testNodeIterator() {
        when(nodeConsumer.test(anyLong())).thenReturn(true);
        final PrimitiveLongIterator iterator = graph.nodeIterator();
        while(iterator.hasNext()) {
            nodeConsumer.test(iterator.next());
        }
        verify(nodeConsumer, times(1)).test(eq(v0));
        verify(nodeConsumer, times(1)).test(eq(v1));
        verify(nodeConsumer, times(1)).test(eq(v2));
    }

    @Test
    void testV0OutgoingForEach() {
        when(relationConsumer.accept(anyLong(), anyLong())).thenReturn(true);
        graph.forEachRelationship(v0, OUTGOING, relationConsumer);
        verify(relationConsumer, times(2)).accept(anyLong(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v1));
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v2));
    }

    @Test
    void testV1OutgoingForEach() {
        when(relationConsumer.accept(anyLong(), anyLong())).thenReturn(true);
        graph.forEachRelationship(v1, OUTGOING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v2));
    }

    @Test
    void testV2OutgoingForEach() {
        graph.forEachRelationship(v2, OUTGOING, relationConsumer);
        verify(relationConsumer, never()).accept(anyLong(), anyLong());
    }

    @Test
    void testV0IncomingForEach() {
        graph.forEachRelationship(v0, INCOMING, relationConsumer);
        verify(relationConsumer, never()).accept(anyLong(), anyLong());
    }

    @Test
    void testV1IncomingForEach() {
        when(relationConsumer.accept(anyLong(), anyLong())).thenReturn(true);
        graph.forEachRelationship(v1, INCOMING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v0));
    }

    @Test
    void testV2IncomingForEach() {
        when(relationConsumer.accept(anyLong(), anyLong())).thenReturn(true);
        graph.forEachRelationship(v2, INCOMING, relationConsumer);
        verify(relationConsumer, times(2)).accept(anyLong(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v2), eq(v0));
        verify(relationConsumer, times(1)).accept(eq(v2), eq(v1));
    }

    @Test
    void testDegree() {
        assertEquals(3, graph.nodeCount());

        assertEquals(2, graph.degree(v0, OUTGOING));
        assertEquals(0, graph.degree(v0, INCOMING));

        assertEquals(1, graph.degree(v1, OUTGOING));
        assertEquals(1, graph.degree(v1, INCOMING));

        assertEquals(0, graph.degree(v2, OUTGOING));
        assertEquals(2, graph.degree(v2, INCOMING));
    }
}
