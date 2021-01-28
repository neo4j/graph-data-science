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
package org.neo4j.graphalgo.impl.shortestpaths;

import com.carrotsearch.hppc.IntDoubleMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import static java.lang.Math.toIntExact;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**         5     5      5
 *      (1)---(2)---(3)----.
 *    5/ 2\2  2 \2  2 \2  2 \
 *  (S)---(7)---(8)---(9)---(X)--//->(S)
 *    3\  /3 3  /3 3  /3 3  /
 *      (4)---(5)---(6)----°
 *
 * S->X: {S,G,H,I,X}:8, {S,D,E,F,X}:12, {S,A,B,C,X}:20
 */
@GdlExtension
final class ShortestPathsTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (s:Node)" +
        ", (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +
        ", (x:Node)" +
        ", (q:Node)" + // outstanding node

        ", (s)-[:TYPE {cost:5}]->(a)" +
        ", (a)-[:TYPE {cost:5}]->(b)" +
        ", (b)-[:TYPE {cost:5}]->(c)" +
        ", (c)-[:TYPE {cost:5}]->(x)" +

        ", (a)-[:TYPE {cost:2}]->(g)" +
        ", (b)-[:TYPE {cost:2}]->(h)" +
        ", (c)-[:TYPE {cost:2}]->(i)" +

        ", (s)-[:TYPE {cost:3}]->(d)" +
        ", (d)-[:TYPE {cost:3}]->(e)" +
        ", (e)-[:TYPE {cost:3}]->(f)" +
        ", (f)-[:TYPE {cost:3}]->(x)" +

        ", (d)-[:TYPE {cost:3}]->(g)" +
        ", (e)-[:TYPE {cost:3}]->(h)" +
        ", (f)-[:TYPE {cost:3}]->(i)" +

        ", (s)-[:TYPE {cost:2}]->(g)" +
        ", (g)-[:TYPE {cost:2}]->(h)" +
        ", (h)-[:TYPE {cost:2}]->(i)" +
        ", (i)-[:TYPE {cost:2}]->(x)" +

        ", (x)-[:TYPE {cost:2}]->(s)"; // create cycle

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    private long head, tail, outstanding;

    @BeforeEach
    void setup() {
        head = idFunction.of("s");
        tail = idFunction.of("x");
        outstanding = idFunction.of("q");
    }

    @Test
    void testPaths() {
        ShortestPaths sssp = new ShortestPaths(graph, head);
        IntDoubleMap sp = sssp.compute().getShortestPaths();

        assertEquals(8, sp.get(toIntExact(graph.toMappedNodeId(tail))), 0.1);
        assertEquals(Double.POSITIVE_INFINITY, sp.get(toIntExact(graph.toMappedNodeId(outstanding))), 0.1);
    }

    @Nested
    class WithoutRelationshipProperties {

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE " +
            "  (s:Node)" +
            ", (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (e:Node)" +
            ", (f:Node)" +
            ", (g:Node)" +
            ", (h:Node)" +
            ", (i:Node)" +
            ", (x:Node)" +
            ", (q:Node)" + // outstanding node

            ", (s)-[:TYPE]->(a)" +
            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(x)" +

            ", (a)-[:TYPE]->(g)" +
            ", (b)-[:TYPE]->(h)" +
            ", (c)-[:TYPE]->(i)" +

            ", (s)-[:TYPE]->(d)" +
            ", (d)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(x)" +

            ", (d)-[:TYPE]->(g)" +
            ", (e)-[:TYPE]->(h)" +
            ", (f)-[:TYPE]->(i)" +

            ", (s)-[:TYPE]->(g)" +
            ", (g)-[:TYPE]->(h)" +
            ", (h)-[:TYPE]->(i)" +
            ", (i)-[:TYPE]->(x)" +

            ", (x)-[:TYPE]->(s)"; // create cycle

        @Test
        void testPathsWithDefaultCost() {

            ShortestPaths sssp = new ShortestPaths(graph, head);
            IntDoubleMap sp = sssp.compute().getShortestPaths();

            assertEquals(4, sp.get(toIntExact(graph.toMappedNodeId(tail))), 0.1);
            assertEquals(Double.POSITIVE_INFINITY, sp.get(toIntExact(graph.toMappedNodeId(outstanding))), 0.1);
        }
    }

}
