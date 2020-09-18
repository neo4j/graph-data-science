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
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

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
@GdlExtension
class YensTest {

    private static final double DELTA = 0.001;

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +

        ", (a)-[:REL {cost:1.0}]->(b)," +
        " (b)-[:REL {cost:1.0}]->(c)," +
        " (c)-[:REL {cost:1.0}]->(d)," +
        " (e)-[:REL {cost:1.0}]->(d)," +
        " (a)-[:REL {cost:1.0}]->(e)," +

        " (a)-[:REL {cost:5.0}]->(f)," +
        " (b)-[:REL {cost:4.0}]->(f)," +
        " (c)-[:REL {cost:1.0}]->(f)," +
        " (d)-[:REL {cost:1.0}]->(f)," +
        " (e)-[:REL {cost:4.0}]->(f)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void test() {
        YensKShortestPaths yens = new YensKShortestPaths(
            graph,
            idOf("a"),
            idOf("f"),
            42,
            10
        ).compute();

        List<WeightedPath> paths = yens.getPaths();
        DoubleConsumer mock = mock(DoubleConsumer.class);
        for (int i = 0; i < paths.size(); i++) {
            final WeightedPath path = paths.get(i);
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
                idOf("a"), idOf("f"),
                idOf("e"), idOf("f"),
                idOf("d"), idOf("f"),
                idOf("a"), idOf("b"));
        final Optional<WeightedPath> path = new YensKShortestPathsDijkstra(graph)
                .withFilter(filter04325)
                .compute(idOf("a"), idOf("f"));
        assertTrue(path.isPresent());
        final WeightedPath weightedPath = path.get();
        assertEquals(4., weightedPath.getCost(), DELTA);
        assertArrayEquals(
                new int[]{idOf("a"), idOf("e"), idOf("d"), idOf("c"), idOf("f")},
                weightedPath.toArray());
    }

    @Test
    void test01235() {
        final RelationshipConsumer filter01235 = filter(
                idOf("a"), idOf("f"),
                idOf("b"), idOf("f"),
                idOf("c"), idOf("f"),
                idOf("a"), idOf("e"));
        final Optional<WeightedPath> path = new YensKShortestPathsDijkstra(graph)
                .withFilter(filter01235)
                .compute(idOf("a"), idOf("f"));
        assertTrue(path.isPresent());
        final WeightedPath weightedPath = path.get();
        assertEquals(4., weightedPath.getCost(), DELTA);
        assertArrayEquals(
                new int[]{idOf("a"), idOf("b"), idOf("c"), idOf("d"), idOf("f")},
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

    private int idOf(String node) {
        return (int) idFunction.of(node);
    }
}
