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
package org.neo4j.gds.impl.spanningTree;

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
        final SpanningTree spanningTree = new KSpanningTree(graph, graph, graph, Prim.MAX_OPERATOR, a, 2, ProgressTracker.NULL_TRACKER)
                .compute();

        assertEquals(spanningTree.head(a), spanningTree.head(b));
        assertEquals(spanningTree.head(c), spanningTree.head(d));
        assertNotEquals(spanningTree.head(a), spanningTree.head(c));
        assertNotEquals(spanningTree.head(a), spanningTree.head(x));
        assertNotEquals(spanningTree.head(c), spanningTree.head(x));
    }

    @Test
    void testMinimumKSpanningTree() {
        final SpanningTree spanningTree = new KSpanningTree(graph, graph, graph, Prim.MIN_OPERATOR, a, 2, ProgressTracker.NULL_TRACKER)
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
        SpanningTree spanningTree = new KSpanningTree(graph, graph, graph, Prim.MIN_OPERATOR, 0, 2, ProgressTracker.NULL_TRACKER)
            .compute();

        SpanningTree otherSpanningTree = new KSpanningTree(graph, graph, graph, Prim.MIN_OPERATOR, 5, 2, ProgressTracker.NULL_TRACKER)
            .compute();

        assertEquals(spanningTree, otherSpanningTree);
    }
}
