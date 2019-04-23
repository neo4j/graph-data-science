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
package org.neo4j.graphalgo.core.huge;

import com.carrotsearch.hppc.IntArrayList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class LoadingTest {

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.<Object[]>asList(
                new Object[]{HeavyGraphFactory.class, "heavy"},
                new Object[]{HugeGraphFactory.class, "huge"}
        );
    }

    private final Class<? extends GraphFactory> graphImpl;

    @SuppressWarnings("unused")
    public LoadingTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void testBasicLoading() {
        db.execute("CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node2 {name:'d'})\n" +
                "CREATE (e:Node2 {name:'e'})\n" +

                "CREATE" +
                " (a)-[:TYPE {prop:1}]->(b),\n" +
                " (e)-[:TYPE {prop:2}]->(d),\n" +
                " (d)-[:TYPE {prop:3}]->(c),\n" +
                " (a)-[:TYPE {prop:4}]->(c),\n" +
                " (a)-[:TYPE {prop:5}]->(d),\n" +
                " (a)-[:TYPE2 {prop:6}]->(d),\n" +
                " (b)-[:TYPE2 {prop:7}]->(e),\n" +
                " (a)-[:TYPE2 {prop:8}]->(e)");

        final Graph graph = new GraphLoader(db)
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .load(graphImpl);

        assertEquals(3, graph.nodeCount());

        assertEquals(2, graph.degree(0, Direction.OUTGOING));
        assertEquals(0, graph.degree(1, Direction.OUTGOING));
        assertEquals(0, graph.degree(2, Direction.OUTGOING));

        checkRels(graph, 0, 1, 2);
        checkRels(graph, 1);
        checkRels(graph, 2);
    }

    private void checkRels(Graph graph, int node, int... expected) {
        Arrays.sort(expected);
        assertArrayEquals(expected, mkRels(graph, node));
    }

    private int[] mkRels(Graph graph, int node) {
        final IntArrayList rels = new IntArrayList();
        graph.forEachOutgoing(node, (s, t, r) -> {
            rels.add(t);
            return true;
        });
        int[] ids = rels.toArray();
        Arrays.sort(ids);
        return ids;
    }
}
