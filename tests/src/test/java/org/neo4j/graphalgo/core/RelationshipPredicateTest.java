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
package org.neo4j.graphalgo.core;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 *      (A)-->(B)-->(C)
 *       ^           |
 *       °-----------°
 *
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class RelationshipPredicateTest {

    public static final Label LABEL = Label.label("Node");

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static long nodeA;
    private static long nodeB;
    private static long nodeC;

    private final Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy", HeavyGraphFactory.class},
                new Object[]{"Huge", HugeGraphFactory.class},
                new Object[]{"View", GraphViewFactory.class}
        );
    }

    public RelationshipPredicateTest(
            String ignoredNameForNiceTestDisplay,
            Class<? extends GraphFactory> graphImpl) {
        this.graphImpl = graphImpl;
    }

    @BeforeClass
    public static void setupGraph() throws KernelException {
        DB.execute("CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE" +
                " (a)-[:TYPE]->(b),\n" +
                " (b)-[:TYPE]->(c),\n" +
                " (c)-[:TYPE]->(a)");

        try (Transaction transaction = DB.beginTx()) {
            nodeA = DB.findNode(LABEL, "name", "a").getId();
            nodeB = DB.findNode(LABEL, "name", "b").getId();
            nodeC = DB.findNode(LABEL, "name", "c").getId();
            transaction.success();
        };
    }

    @Test
    public void testOutgoing() throws Exception {

        final Graph graph = loader()
                .withDirection(Direction.OUTGOING)
                .withSort(true)
                .load(HeavyGraphFactory.class);

        // A -> B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB)
        ));

        // B -> A
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA)
        ));


        // B -> C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC)
        ));

        // C -> B
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB)
        ));


        // C -> A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA)
        ));

        // A -> C
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC)
        ));
    }


    @Test
    public void testIncoming() throws Exception {

        final Graph graph = loader()
                .withDirection(Direction.INCOMING)
                .withSort(true)
                .load(HeavyGraphFactory.class);

        // B <- A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA),
                Direction.INCOMING
        ));

        // A <- B
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB),
                Direction.INCOMING
        ));



        // C <- B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB),
                Direction.INCOMING
        ));

        // B <- C
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC),
                Direction.INCOMING
        ));


        // A <- C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC),
                Direction.INCOMING
        ));

        // C <- A
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA),
                Direction.INCOMING
        ));
    }


    @Test
    public void testBoth() throws Exception {

        final Graph graph = loader()
                .withDirection(Direction.BOTH)
                .withSort(true)
                .load(HeavyGraphFactory.class);

        // A -> B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB)
        ));

        // B -> A
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA)
        ));


        // B -> C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC)
        ));

        // C -> B
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB)
        ));



        // C -> A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA)
        ));

        // A -> C
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC)
        ));


        // B <- A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA),
                Direction.INCOMING
        ));

        // A <- B
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB),
                Direction.INCOMING
        ));

        // C <- B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB),
                Direction.INCOMING
        ));

        // B <- C
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC),
                Direction.INCOMING
        ));



        // A <- C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC),
                Direction.INCOMING
        ));

        // C <- A
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA),
                Direction.INCOMING
        ));


        // A <-> B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB),
                Direction.BOTH
        ));

        // B <-> A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA),
                Direction.BOTH
        ));


        // B <-> C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC),
                Direction.BOTH
        ));

        // C <-> B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB),
                Direction.BOTH
        ));


        // C <-> A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA),
                Direction.BOTH
        ));

        // A <-> C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC),
                Direction.BOTH
        ));
    }

    private GraphLoader loader() {
        return new GraphLoader(DB)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .withoutNodeProperties();
    }
}
