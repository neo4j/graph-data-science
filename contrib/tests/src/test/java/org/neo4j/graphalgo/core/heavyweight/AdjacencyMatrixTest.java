/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.heavyweight;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * @author mknobloch
 */
@RunWith(MockitoJUnitRunner.class)
public class AdjacencyMatrixTest {

    private static AdjacencyMatrix matrix;

    private static RelationshipConsumer relationConsumer = mock(RelationshipConsumer.class);

    @AfterClass
    public static void tearDown() throws Exception {
        matrix = null;
    }

    @BeforeClass
    public static void setup() {

        matrix = new AdjacencyMatrix(3, false, AllocationTracker.EMPTY);

        // 0 -> {1, 2}
        matrix.armOut(0, 2);
        matrix.addOutgoing(0, 1);
        matrix.addOutgoing(0, 2);

        // 1 -> {2}
        matrix.armOut(1, 1);
        matrix.addOutgoing(1, 2);

        // 2 -> {}
        matrix.armOut(2, 0);

        // 0 <- {}
        matrix.armIn(0, 0);

        // 1 <- {0}
        matrix.armIn(1, 1);
        matrix.addIncoming(0, 1);

        // 2 <- {0, 1}
        matrix.armIn(2, 2);
        matrix.addIncoming(0, 2);
        matrix.addIncoming(1, 2);
    }

    @Before
    public void resetMocks() {
        Mockito.reset(relationConsumer);
    }

    @Test
    public void testOutgoingDegree() throws Exception {
        assertEquals(2, matrix.degree(0, OUTGOING));
        assertEquals(1, matrix.degree(1, OUTGOING));
        assertEquals(0, matrix.degree(2, OUTGOING));
    }

    @Test
    public void testIncomingDegree() throws Exception {
        assertEquals(0, matrix.degree(0, INCOMING));
        assertEquals(1, matrix.degree(1, INCOMING));
        assertEquals(2, matrix.degree(2, INCOMING));
    }

    @Test
    public void testV0Outgoing() throws Exception {
        matrix.forEach(0, OUTGOING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(0), eq(1), eq(RawValues.combineIntInt(0, 1)));
        verify(relationConsumer, times(1)).accept(eq(0), eq(2), eq(RawValues.combineIntInt(0, 2)));
    }

    @Test
    public void testV1Outgoing() throws Exception {
        matrix.forEach(1, OUTGOING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(1), eq(2), eq(RawValues.combineIntInt(1, 2)));
    }

    @Test
    public void testV2Outgoing() throws Exception {
        matrix.forEach(2, OUTGOING, relationConsumer);
        verify(relationConsumer, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV0Incoming() throws Exception {
        matrix.forEach(0, INCOMING, relationConsumer);
        verify(relationConsumer, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV1Incoming() throws Exception {
        matrix.forEach(1, INCOMING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(1), eq(0), eq(RawValues.combineIntInt(0, 1)));
    }

    @Test
    public void testV2Incoming() throws Exception {
        matrix.forEach(2, INCOMING, relationConsumer);
        verify(relationConsumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(2), eq(0), eq(RawValues.combineIntInt(0, 2)));
        verify(relationConsumer, times(1)).accept(eq(2), eq(1), eq(RawValues.combineIntInt(1, 2)));
    }
}
