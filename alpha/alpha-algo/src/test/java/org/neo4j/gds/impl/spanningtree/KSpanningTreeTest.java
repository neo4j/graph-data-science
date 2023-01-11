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
package org.neo4j.gds.impl.spanningtree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningTree;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 *          1
 *  (x) >(a)---(d)    (x)  (a)   (d)
 *      /3 \2 /3   =>     /     /
 *    (b)---(c)         (b)   (c)
 *        1
 */
@GdlExtension
class KSpanningTreeTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
            "CREATE " +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (x:Node)" +

            ", (a)-[:TYPE {w: 3.0}]->(b)" +
            ", (a)-[:TYPE {w: 2.0}]->(c)" +
            ", (a)-[:TYPE {w: 1.0}]->(d)" +
            ", (b)-[:TYPE {w: 1.0}]->(c)" +
            ", (d)-[:TYPE {w: 3.0}]->(c)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    private int a, b, c, d, x;

    @BeforeEach
    void setUp() {
        a = (int) idFunction.of("a");
        b = (int) idFunction.of("b");
        c = (int) idFunction.of("c");
        d = (int) idFunction.of("d");
        x = (int) idFunction.of("x");
    }

    @Test
    void testMaximumKSpanningTree() {
        final SpanningTree spanningTree = new KSpanningTree(
            graph,
            Prim.MAX_OPERATOR,
            a,
            2,
            ProgressTracker.NULL_TRACKER
        )
            .compute();

        assertEquals(spanningTree.head(a), spanningTree.head(b));
        assertEquals(spanningTree.head(c), spanningTree.head(d));
        assertNotEquals(spanningTree.head(a), spanningTree.head(c));
        assertNotEquals(spanningTree.head(a), spanningTree.head(x));
        assertNotEquals(spanningTree.head(c), spanningTree.head(x));
    }

    @Test
    void testMinimumKSpanningTree() {
        final SpanningTree spanningTree = new KSpanningTree(
            graph,
            Prim.MIN_OPERATOR,
            a,
            2,
            ProgressTracker.NULL_TRACKER
        )
            .compute();

        assertEquals(spanningTree.head(a), spanningTree.head(d));
        assertEquals(spanningTree.head(b), spanningTree.head(c));
        assertNotEquals(spanningTree.head(a), spanningTree.head(b));
        assertNotEquals(spanningTree.head(a), spanningTree.head(x));
        assertNotEquals(spanningTree.head(b), spanningTree.head(x));
    }

    @Test
    @Disabled("Need to extend GdlGraph to generate offset node IDs and fix the test")
    void testNeoIdsWithOffset() {
        SpanningTree spanningTree = new KSpanningTree(graph, Prim.MIN_OPERATOR, 0, 2, ProgressTracker.NULL_TRACKER)
            .compute();

        SpanningTree otherSpanningTree = new KSpanningTree(graph, Prim.MIN_OPERATOR, 5, 2, ProgressTracker.NULL_TRACKER)
            .compute();

        assertEquals(spanningTree, otherSpanningTree);
    }

    @Test
    void shouldProduceSingleConnectedTree() {
        var factory = GdlFactory.of("CREATE" +
                                    "  (a:Node)" +
                                    ", (b:Node)" +
                                    ", (c:Node)" +
                                    ", (d:Node)" +
                                    ", (e:Node)" +
                                    ", (a)-[:TYPE {cost: 1.0}]->(b)" +
                                    ", (b)-[:TYPE {cost: 20.0}]->(c)" +
                                    ", (c)-[:TYPE {cost: 30.0}]->(d)" +
                                    ", (d)-[:TYPE {cost: 1.0}]->(e)"
        );
        var graph = factory.build().getUnion();
        var startNode = factory.nodeId("a");

        var k = 3;
        var spanningTree = new KSpanningTree(
            graph,
            Prim.MIN_OPERATOR,
            startNode,
            k,
            ProgressTracker.NULL_TRACKER
        ).compute();

        // if there are more than k nodes then there is more than one root
        // meaning there is more than one tree (or the tree is broken)
        var nodesInTree = new HashSet<Long>();
        spanningTree.forEach((s, t, __) -> {
            nodesInTree.add(s);
            nodesInTree.add(t);
            return true;
        });

        assertThat(nodesInTree.size()).isEqualTo(k);
    }

    @Test
    void shouldProduceSingleTreeWithKMinusOneEdges() {
        var factory = GdlFactory.of("CREATE" +
                                    "  (a:Node)" +
                                    ", (b:Node)" +
                                    ", (c:Node)" +
                                    ", (d:Node)" +
                                    ", (e:Node)" +
                                    ", (f:Node)" +
                                    ", (a)-[:TYPE {cost: 1.0}]->(b)" +
                                    ", (b)-[:TYPE {cost: 20.0}]->(c)" +
                                    ", (c)-[:TYPE {cost: 30.0}]->(d)" +
                                    ", (d)-[:TYPE {cost: 1.0}]->(e)" +
                                    ", (e)-[:TYPE {cost: 1.0}]->(f)"
        );
        var graph = factory.build().getUnion();
        var startNode = factory.nodeId("a");

        var k = 2;

        var spanningTree = new KSpanningTree(
            graph,
            Prim.MIN_OPERATOR,
            startNode,
            2,
            ProgressTracker.NULL_TRACKER
        ).compute();

        var counter = new MutableLong(0);
        spanningTree.forEach((__, ___, ____) -> {
            counter.add(1);
            return true;
        });

        assertThat(counter.getValue()).isEqualTo(k - 1);
    }
}
