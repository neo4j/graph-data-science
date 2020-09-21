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
package org.neo4j.graphalgo.impl.shortestpaths;

import com.carrotsearch.hppc.LongArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

/**
 * Test for specialized dijkstra implementation with filters & maxDepth
 *
 *
 * Graph:
 *
 *     (b)   (e)
 *   2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 *   1\ 2/ 1\ 2/
 *    (c)   (f)
 */
@GdlExtension
class DijkstraTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node) " +
        ", (b:Node) " +
        ", (c:Node) " +
        ", (d:Node) " +
        ", (e:Node) " +
        ", (f:Node) " +
        ", (g:Node) " +

        "  (a)-[:TYPE {cost:2.0}]->(b)" +
        ", (a)-[:TYPE {cost:1.0}]->(c)" +
        ", (b)-[:TYPE {cost:1.0}]->(d)" +
        ", (c)-[:TYPE {cost:2.0}]->(d)" +
        ", (d)-[:TYPE {cost:1.0}]->(e)" +
        ", (d)-[:TYPE {cost:2.0}]->(f)" +
        ", (e)-[:TYPE {cost:2.0}]->(g)" +
        ", (f)-[:TYPE {cost:1.0}]->(g)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    private static LongArrayList edgeBlackList;
    private static YensKShortestPathsDijkstra dijkstra;

    @BeforeEach
    void setupGraph() {
        edgeBlackList = new LongArrayList();

        dijkstra = new YensKShortestPathsDijkstra(graph)
                .withFilter(longToIntConsumer((s, t) -> !edgeBlackList.contains(RawValues.combineIntInt(s, t))));
    }

    private int id(String name) {
        return Math.toIntExact(graph.toMappedNodeId(idFunction.of(name)));
    }

    @Test
    void testNoFilter() {
        edgeBlackList.clear();
        final WeightedPath weightedPath = dijkstra();
        assertEquals(5, weightedPath.size());
    }

    @Test
    void testFilterACDF() {
        edgeBlackList.clear();
        edgeBlackList.add(RawValues.combineIntInt(id("a"), id("c")));
        edgeBlackList.add(RawValues.combineIntInt(id("d"), id("f")));
        final WeightedPath weightedPath = dijkstra();
        assertEquals(5, weightedPath.size());
        assertTrue(weightedPath.containsNode(id("b")));
        assertTrue(weightedPath.containsNode(id("e")));
    }

    @Test
    void testFilterABDE() {
        edgeBlackList.clear();
        edgeBlackList.add(RawValues.combineIntInt(id("a"), id("b")));
        edgeBlackList.add(RawValues.combineIntInt(id("d"), id("e")));
        final WeightedPath weightedPath = dijkstra();
        assertEquals(5, weightedPath.size());
        assertTrue(weightedPath.containsNode(id("c")));
        assertTrue(weightedPath.containsNode(id("f")));
    }

    @Test
    void testMaxDepth() {
        assertTrue(dijkstra.compute(id("a"), id("d"), 4).isPresent());
        assertFalse(dijkstra.compute(id("a"), id("d"), 3).isPresent());
    }

    private WeightedPath dijkstra() {
        return dijkstra.compute(id("a"), id("g"))
                .orElseThrow(() -> new AssertionError("No path"));
    }

}
