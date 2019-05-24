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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.impl.spanningTrees.Prim;
import org.neo4j.graphalgo.impl.spanningTrees.SpanningTree;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


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
 *
 * @author mknobloch
 */
@RunWith(Parameterized.class)
public class PrimTest {

    private final Label label;
    private int a, b, c, d, e, y, z;

    private static final String cypher =
            "CREATE (a:Node {name:'a'})\n" +
            "CREATE (b:Node {name:'b'})\n" +
            "CREATE (c:Node {name:'c'})\n" +
            "CREATE (d:Node {name:'d'})\n" +
            "CREATE (e:Node {name:'e'})\n" +
            "CREATE (y:Node {name:'y'})\n" +
            "CREATE (z:Node {name:'z'})\n" +
            "CREATE" +
            " (a)-[:TYPE {cost:1.0}]->(b),\n" +
            " (a)-[:TYPE {cost:2.0}]->(c),\n" +
            " (b)-[:TYPE {cost:3.0}]->(c),\n" +
            " (b)-[:TYPE {cost:4.0}]->(d),\n" +
            " (c)-[:TYPE {cost:5.0}]->(e),\n" +
            " (d)-[:TYPE {cost:6.0}]->(e)";

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private final Graph graph;

    @BeforeClass
    public static void setupGraph() {
        try (Transaction tx = DB.beginTx()) {
            DB.execute(cypher);
            tx.success();
        }
    }


    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "Heavy"},
                new Object[]{HugeGraphFactory.class, "Huge"}
        );
    }


    public PrimTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {

        label = Label.label("Node");

        graph = new GraphLoader(DB)
                .withLabel(label)
                .withRelationshipType("TYPE")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withoutNodeWeights()
                .asUndirected(true)
                .load(graphImpl);

        try (Transaction transaction = DB.beginTx()) {
            a = Math.toIntExact(graph.toMappedNodeId(DB.findNode(label, "name", "a").getId()));
            b = Math.toIntExact(graph.toMappedNodeId(DB.findNode(label, "name", "b").getId()));
            c = Math.toIntExact(graph.toMappedNodeId(DB.findNode(label, "name", "c").getId()));
            d = Math.toIntExact(graph.toMappedNodeId(DB.findNode(label, "name", "d").getId()));
            e = Math.toIntExact(graph.toMappedNodeId(DB.findNode(label, "name", "e").getId()));
            y = Math.toIntExact(graph.toMappedNodeId(DB.findNode(label, "name", "y").getId()));
            z = Math.toIntExact(graph.toMappedNodeId(DB.findNode(label, "name", "z").getId()));
            transaction.success();
        }
    }

    @Test
    public void testMaximumFromA() throws Exception {
        assertMaximum(new Prim(graph, graph, graph).computeMaximumSpanningTree(a).getSpanningTree());
    }

    @Test
    public void testMaximumFromB() throws Exception {
        assertMaximum(new Prim(graph, graph, graph).computeMaximumSpanningTree(b).getSpanningTree());
    }

    @Test
    public void testMaximumFromC() throws Exception {
        assertMaximum(new Prim(graph, graph, graph).computeMaximumSpanningTree(c).getSpanningTree());
    }

    @Test
    public void testMaximumFromD() throws Exception {
        assertMaximum(new Prim(graph, graph, graph).computeMaximumSpanningTree(d).getSpanningTree());
    }

    @Test
    public void testMaximumFromE() throws Exception {
        assertMaximum(new Prim(graph, graph, graph).computeMaximumSpanningTree(e).getSpanningTree());
    }

    @Test
    public void testMinimumFromA() throws Exception {
        assertMinimum(new Prim(graph, graph, graph).computeMinimumSpanningTree(a).getSpanningTree());
    }

    @Test
    public void testMinimumFromB() throws Exception {
        assertMinimum(new Prim(graph, graph, graph).computeMinimumSpanningTree(b).getSpanningTree());
    }

    @Test
    public void testMinimumFromC() throws Exception {
        assertMinimum(new Prim(graph, graph, graph).computeMinimumSpanningTree(c).getSpanningTree());
    }

    @Test
    public void testMinimumFromD() throws Exception {
        assertMinimum(new Prim(graph, graph, graph).computeMinimumSpanningTree(d).getSpanningTree());
    }

    @Test
    public void testMinimumFromE() throws Exception {
        assertMinimum(new Prim(graph, graph, graph).computeMinimumSpanningTree(d).getSpanningTree());
    }

    private void assertMinimum(SpanningTree mst) {
        assertEquals(5, mst.effectiveNodeCount);
        assertEquals(-1 , mst.parent[y]);
        assertEquals(-1 , mst.parent[z]);
    }

    private void assertMaximum(SpanningTree mst) {
        assertEquals(5, mst.effectiveNodeCount);
        assertEquals(-1 , mst.parent[y]);
        assertEquals(-1 , mst.parent[z]);
    }

}
