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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.LongArrayList;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.impl.yens.Dijkstra;
import org.neo4j.graphalgo.impl.yens.WeightedPath;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.List;
import java.util.Optional;
import java.util.function.DoubleConsumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphalgo.core.utils.Converters.longToIntConsumer;

/**
 * Graph:
 *
 *            (0)
 *          /  |  \
 *       (4)--(5)--(1)
 *         \  /  \ /
 *         (3)---(2)
 *
 * @author mknblch
 */
public class YensTest {

    public static final double DELTA = 0.001;

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE {cost:1.0}]->(b),\n" +
                        " (b)-[:TYPE {cost:1.0}]->(c),\n" +
                        " (c)-[:TYPE {cost:1.0}]->(d),\n" +
                        " (e)-[:TYPE {cost:1.0}]->(d),\n" +
                        " (a)-[:TYPE {cost:1.0}]->(e),\n" +

                        " (a)-[:TYPE {cost:5.0}]->(f),\n" +
                        " (b)-[:TYPE {cost:4.0}]->(f),\n" +
                        " (c)-[:TYPE {cost:1.0}]->(f),\n" +
                        " (d)-[:TYPE {cost:1.0}]->(f),\n" +
                        " (e)-[:TYPE {cost:4.0}]->(f)";

        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .undirected()
                .load(HugeGraphFactory.class);
    }

    @Test
    public void test() throws Exception {
        final YensKShortestPaths yens = new YensKShortestPaths(graph)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .compute(id("a"), id("f"), Direction.OUTGOING, 42, 10);
        final List<WeightedPath> paths = yens.getPaths();
        final DoubleConsumer mock = mock(DoubleConsumer.class);
        for (int i = 0; i < paths.size(); i++) {
            final WeightedPath path = paths.get(i);
            System.out.println("path " + path + " : " + path.getCost());
            mock.accept(path.getCost());
        }
        verify(mock, times(2)).accept(eq(3.0, DELTA));
        verify(mock, times(2)).accept(eq(4.0, DELTA));
        verify(mock, times(3)).accept(eq(5.0, DELTA));
        verify(mock, times(2)).accept(eq(8.0, DELTA));
    }

    @Test
    public void test04325() throws Exception {
        final RelationshipConsumer filter04325 = filter(
                id("a"), id("f"),
                id("e"), id("f"),
                id("d"), id("f"),
                id("a"), id("b"));
        final Optional<WeightedPath> path = new Dijkstra(graph)
                .withDirection(Direction.OUTGOING)
                .withFilter(filter04325)
                .compute(id("a"), id("f"));
        assertTrue(path.isPresent());
        final WeightedPath weightedPath = path.get();
        assertEquals(4., weightedPath.getCost(), DELTA);
        assertArrayEquals(
                new int[]{id("a"), id("e"), id("d"), id("c"), id("f")},
                weightedPath.toArray());
    }

    @Test
    public void test01235() throws Exception {
        final RelationshipConsumer filter01235 = filter(
                id("a"), id("f"),
                id("b"), id("f"),
                id("c"), id("f"),
                id("a"), id("e"));
        final Optional<WeightedPath> path = new Dijkstra(graph)
                .withDirection(Direction.OUTGOING)
                .withFilter(filter01235)
                .compute(id("a"), id("f"));
        assertTrue(path.isPresent());
        final WeightedPath weightedPath = path.get();
        assertEquals(4., weightedPath.getCost(), DELTA);
        assertArrayEquals(
                new int[]{id("a"), id("b"), id("c"), id("d"), id("f")},
                weightedPath.toArray());
    }

    private static RelationshipConsumer filter(int... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("Invalid count of pair elements");
        }
        final LongArrayList list = new LongArrayList(pairs.length / 2);
        for (int i = 0; i < pairs.length; i += 2) {
            list.add(RawValues.combineIntInt(pairs[i], pairs[i + 1]));
        }
        return longToIntConsumer((s, t) -> !list.contains(RawValues.combineIntInt(s, t)));
    }

    private int id(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return Math.toIntExact(graph.toMappedNodeId(node[0].getId()));
    }
}
