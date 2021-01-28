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

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * expected path OUTGOING:  abcf
 *               INCOMING:  adef
 *               BOTH:      adef
 *
 * x should be unreachable
 *     2    2   2
 *   ,->(b)->(c)->(f)
 *  |  1    1    1 |   (x) // unreachable
 * (a)<-(d)<-(e)<-´
 */
@GdlExtension
public class DirectedDijkstraSPTest {

    @GdlGraph(graphNamePrefix = "natural", orientation = Orientation.NATURAL)
    @GdlGraph(graphNamePrefix = "reverse", orientation = Orientation.REVERSE)
    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED, aggregation = Aggregation.SINGLE)
    public static final String DB_CYPHER =
        "CREATE " +
        "  (d:Node)" +
        ", (a:Node)" +
        ", (c:Node)" +
        ", (b:Node)" +
        ", (f:Node)" +
        ", (e:Node)" +
        ", (x:Node)" +

        "  (a)-[:REL {cost:2}]->(b)" +
        ", (b)-[:REL {cost:2}]->(c)" +
        ", (c)-[:REL {cost:2}]->(f)" +
        ", (f)-[:REL {cost:1}]->(e)" +
        ", (e)-[:REL {cost:1}]->(d)" +
        ", (d)-[:REL {cost:1}]->(a)";


    @Inject
    private Graph naturalGraph;

    @Inject
    private IdFunction naturalIdFunction;

    @Inject
    private Graph reverseGraph;

    @Inject
    private IdFunction reverseIdFunction;

    @Inject
    private Graph undirectedGraph;

    @Inject
    private IdFunction undirectedIdFunction;

    @Test
    void testOutgoing() {
        DijkstraConfig config = DijkstraConfig.of(naturalIdFunction.of("a"), naturalIdFunction.of("f"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(naturalGraph, config);
        dijkstra.compute();

        assertPath(dijkstra.getFinalPath(), naturalIdFunction, "a", "b", "c", "f");

        assertEquals(6.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    void testIncoming() {
        StringBuilder path = new StringBuilder();
        DijkstraConfig config = DijkstraConfig.of(reverseIdFunction.of("a"), reverseIdFunction.of("f"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(reverseGraph, config);
        dijkstra.compute();

        assertPath(dijkstra.getFinalPath(), reverseIdFunction, "a", "d", "e", "f");
        assertEquals(3.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    void testBoth() {
        DijkstraConfig config = DijkstraConfig.of(undirectedIdFunction.of("a"), undirectedIdFunction.of("f"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(undirectedGraph, config);
        dijkstra.compute();

        assertPath(dijkstra.getFinalPath(), undirectedIdFunction, "a", "d", "e", "f");
        assertEquals(3.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    void testUnreachableOutgoing() {
        DijkstraConfig config = DijkstraConfig.of(naturalIdFunction.of("a"), naturalIdFunction.of("x"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(naturalGraph, config);
        dijkstra.compute();

        assertThat(dijkstra.getFinalPath()).isEmpty();
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }

    @Test
    void testUnreachableIncoming() {
        DijkstraConfig config = DijkstraConfig.of(reverseIdFunction.of("a"), reverseIdFunction.of("x"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(reverseGraph, config);
        dijkstra.compute();

        assertThat(dijkstra.getFinalPath()).isEmpty();
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }

    @Test
    void testUnreachableBoth() {
        DijkstraConfig config = DijkstraConfig.of(undirectedIdFunction.of("a"), undirectedIdFunction.of("x"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(undirectedGraph, config);
        dijkstra.compute();

        assertThat(dijkstra.getFinalPath()).isEmpty();
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }


    private void assertPath(IntContainer actualPath, IdFunction idFunction, String... nodes) {
        StringBuilder actual = new StringBuilder();
        actualPath.forEach((IntProcedure) actual::append);
        StringBuilder expected = new StringBuilder();
        for(var n: nodes) {
            expected.append(idFunction.of(n));
        }

        assertThat(expected).isEqualToIgnoringCase(actual);
    }

}
