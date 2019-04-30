package org.neo4j.graphalgo.core;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;

import java.util.function.LongPredicate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

@RunWith(MockitoJUnitRunner.class)
public abstract class SimpleGraphTestCase extends Neo4JTestCase {

    protected static Graph graph;

    protected static long v0, v1, v2;

    @Mock
    private RelationshipConsumer relationConsumer;

    @Mock
    private LongPredicate nodeConsumer;

    @Before
    public void setupMocks() {
        when(nodeConsumer.test(anyInt())).thenReturn(true);
        when(relationConsumer.accept(anyLong(), anyLong())).thenReturn(true);
    }

    @Test
    public void testForEachNode() throws Exception {

        graph.forEachNode(nodeConsumer);
        verify(nodeConsumer, times(3)).test(anyInt());
        verify(nodeConsumer, times(1)).test(eq(v0));
        verify(nodeConsumer, times(1)).test(eq(v1));
        verify(nodeConsumer, times(1)).test(eq(v2));
    }

    @Test
    public void testNodeIterator() throws Exception {
        final PrimitiveLongIterator iterator = graph.nodeIterator();
        while(iterator.hasNext()) {
            nodeConsumer.test(iterator.next());
        }
        verify(nodeConsumer, times(1)).test(eq(v0));
        verify(nodeConsumer, times(1)).test(eq(v1));
        verify(nodeConsumer, times(1)).test(eq(v2));
    }

    @Test
    public void testV0OutgoingForEach() throws Exception {
        graph.forEachRelationship(v0, OUTGOING, relationConsumer);
        verify(relationConsumer, times(2)).accept(anyLong(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v1));
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v2));
    }

    @Test
    public void testV1OutgoingForEach() throws Exception {
        graph.forEachRelationship(v1, OUTGOING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v2));
    }

    @Test
    public void testV2OutgoingForEach() throws Exception {
        graph.forEachRelationship(v2, OUTGOING, relationConsumer);
        verify(relationConsumer, never()).accept(anyInt(), anyInt());
    }

    @Test
    public void testV0IncomingForEach() throws Exception {
        graph.forEachRelationship(v0, INCOMING, relationConsumer);
        verify(relationConsumer, never()).accept(anyInt(), anyInt());
    }

    @Test
    public void testV1IncomingForEach() throws Exception {
        graph.forEachRelationship(v1, INCOMING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v0));
    }

    @Test
    public void testV2IncomingForEach() throws Exception {
        graph.forEachRelationship(v2, INCOMING, relationConsumer);
        verify(relationConsumer, times(2)).accept(anyInt(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v2), eq(v0));
        verify(relationConsumer, times(1)).accept(eq(v2), eq(v1));
    }

    @Test
    public void testDegree() throws Exception {

        assertEquals(3, graph.nodeCount());

        assertEquals(2, graph.degree(v0, OUTGOING));
        assertEquals(0, graph.degree(v0, INCOMING));

        assertEquals(1, graph.degree(v1, OUTGOING));
        assertEquals(1, graph.degree(v1, INCOMING));

        assertEquals(0, graph.degree(v2, OUTGOING));
        assertEquals(2, graph.degree(v2, INCOMING));
    }
}
