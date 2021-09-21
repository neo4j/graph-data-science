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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.impl.spanningTrees.Prim;
import org.neo4j.gds.impl.spanningTrees.SpanningTree;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Tests if MSTPrim returns a valid tree for each node
 *
 *         a                  a                  a
 *     1 /   \ 2            /  \                  \
 *      /     \            /    \                  \
 *     b --3-- c          b      c          b       c
 *     |       |  =min=>  |      |  =max=>  |       |
 *     4       5          |      |          |       |
 *     |       |          |      |          |       |
 *     d --6-- e          d      e          d-------e
 */
@GdlExtension
class PrimTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (y:Node)" +
        ", (z:Node)" +

        ", (a)-[:TYPE {cost: 1.0}]->(b)" +
        ", (a)-[:TYPE {cost: 2.0}]->(c)" +
        ", (b)-[:TYPE {cost: 3.0}]->(c)" +
        ", (b)-[:TYPE {cost: 4.0}]->(d)" +
        ", (c)-[:TYPE {cost: 5.0}]->(e)" +
        ", (d)-[:TYPE {cost: 6.0}]->(e)";

    private static int a, b, c, d, e, y, z;

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setUp() {
        a = (int) idFunction.of("a");
        b = (int) idFunction.of("b");
        c = (int) idFunction.of("c");
        d = (int) idFunction.of("d");
        e = (int) idFunction.of("e");
        y = (int) idFunction.of("y");
        z = (int) idFunction.of("z");
    }

    @Test
    void testMaximumFromA() {
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, a, ProgressTracker.NULL_TRACKER).compute());
    }

    @Test
    void testMaximumFromB() {
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, b, ProgressTracker.NULL_TRACKER).compute());
    }

    @Test
    void testMaximumFromC() {
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, c, ProgressTracker.NULL_TRACKER).compute());
    }

    @Test
    void testMaximumFromD() {
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, d, ProgressTracker.NULL_TRACKER).compute());
    }

    @Test
    void testMaximumFromE() {
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, e, ProgressTracker.NULL_TRACKER).compute());
    }

    @Test
    void testMinimumFromA() {
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, a, ProgressTracker.NULL_TRACKER).compute());
    }

    @Test
    void testMinimumFromB() {
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, b, ProgressTracker.NULL_TRACKER).compute());
    }

    @Test
    void testMinimumFromC() {
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, c, ProgressTracker.NULL_TRACKER).compute());
    }

    @Test
    void testMinimumFromD() {
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, d, ProgressTracker.NULL_TRACKER).compute());
    }

    @Test
    void testMinimumFromE() {
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, e, ProgressTracker.NULL_TRACKER).compute());
    }

    private void assertMinimum(SpanningTree mst) {
        assertEquals(5, mst.effectiveNodeCount);
        assertEquals(-1, mst.parent[y]);
        assertEquals(-1, mst.parent[z]);
    }

    private void assertMaximum(SpanningTree mst) {
        assertEquals(5, mst.effectiveNodeCount);
        assertEquals(-1, mst.parent[y]);
        assertEquals(-1, mst.parent[z]);
    }
}
