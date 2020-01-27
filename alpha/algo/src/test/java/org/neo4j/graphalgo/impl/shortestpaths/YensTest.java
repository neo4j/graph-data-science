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
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Optional;
import java.util.function.DoubleConsumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

/**
 * Graph:
 *
 *            (0)
 *          /  |  \
 *       (4)--(5)--(1)
 *         \  /  \ /
 *         (3)---(2)
 */
class YensTest extends AlgoTestBase {

    private static final double DELTA = 0.001;

    private Graph graph;

    @BeforeEach
    void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE (f:Node {name:'f'})\n" +
                "CREATE" +
                " (a)-[:REL {cost:1.0}]->(b),\n" +
                " (b)-[:REL {cost:1.0}]->(c),\n" +
                " (c)-[:REL {cost:1.0}]->(d),\n" +
                " (e)-[:REL {cost:1.0}]->(d),\n" +
                " (a)-[:REL {cost:1.0}]->(e),\n" +

                " (a)-[:REL {cost:5.0}]->(f),\n" +
                " (b)-[:REL {cost:4.0}]->(f),\n" +
                " (c)-[:REL {cost:1.0}]->(f),\n" +
                " (d)-[:REL {cost:1.0}]->(f),\n" +
                " (e)-[:REL {cost:4.0}]->(f)";

        runQuery(cypher);

        graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .putRelationshipProjectionsWithIdentifier(
                "REL",
                RelationshipProjection.of("REL", Projection.UNDIRECTED, Aggregation.NONE)
            )
            .addRelationshipProperty(PropertyMapping.of("cost", Double.MAX_VALUE))
            .build()
            .graph(HugeGraphFactory.class);
    }

    @Test
    void test() {
        YensKShortestPaths yens = new YensKShortestPaths(
            graph,
            id("a"),
            id("f"),
            42,
            10
        )
            .withProgressLogger(TestProgressLogger.INSTANCE)
            .compute();
        List<WeightedPath> paths = yens.getPaths();
        DoubleConsumer mock = mock(DoubleConsumer.class);
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
    void test04325() {
        final RelationshipConsumer filter04325 = filter(
                id("a"), id("f"),
                id("e"), id("f"),
                id("d"), id("f"),
                id("a"), id("b"));
        final Optional<WeightedPath> path = new YensKShortestPathsDijkstra(graph)
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
    void test01235() {
        final RelationshipConsumer filter01235 = filter(
                id("a"), id("f"),
                id("b"), id("f"),
                id("c"), id("f"),
                id("a"), id("e"));
        final Optional<WeightedPath> path = new YensKShortestPathsDijkstra(graph)
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
        runQuery("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n", row -> node[0] = row.getNode("n"));
        return Math.toIntExact(graph.toMappedNodeId(node[0].getId()));
    }
}
