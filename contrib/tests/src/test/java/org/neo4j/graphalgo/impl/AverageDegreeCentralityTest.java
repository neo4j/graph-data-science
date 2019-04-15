/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class AverageDegreeCentralityTest {

    private Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
//        return Arrays.asList(
//                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
//                new Object[]{HeavyCypherGraphFactory.class, "HeavyCypherGraphFactory"},
//                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"},
//                new Object[]{GraphViewFactory.class, "GraphViewFactory"}
//        );

        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"}
        );
    }

    private static final String DB_CYPHER = "" +
            "CREATE (_:Label0 {name:\"_\"})\n" +
            "CREATE (a:Label1 {name:\"a\"})\n" +
            "CREATE (b:Label1 {name:\"b\"})\n" +
            "CREATE (c:Label1 {name:\"c\"})\n" +
            "CREATE (d:Label1 {name:\"d\"})\n" +
            "CREATE (e:Label1 {name:\"e\"})\n" +
            "CREATE (f:Label1 {name:\"f\"})\n" +
            "CREATE (g:Label1 {name:\"g\"})\n" +
            "CREATE (h:Label1 {name:\"h\"})\n" +
            "CREATE (i:Label1 {name:\"i\"})\n" +
            "CREATE (j:Label1 {name:\"j\"})\n" +
            "CREATE (k:Label2 {name:\"k\"})\n" +
            "CREATE (l:Label2 {name:\"l\"})\n" +
            "CREATE (m:Label2 {name:\"m\"})\n" +
            "CREATE (n:Label2 {name:\"n\"})\n" +
            "CREATE (o:Label2 {name:\"o\"})\n" +
            "CREATE (p:Label2 {name:\"p\"})\n" +
            "CREATE (q:Label2 {name:\"q\"})\n" +
            "CREATE (r:Label2 {name:\"r\"})\n" +
            "CREATE (s:Label2 {name:\"s\"})\n" +
            "CREATE (t:Label2 {name:\"t\"})\n" +
            "CREATE\n" +
            "  (b)-[:TYPE1 {weight: 2.0}]->(c),\n" +
            "  (c)-[:TYPE1 {weight: 2.0}]->(b),\n" +

            "  (d)-[:TYPE1 {weight: 2.0}]->(a),\n" +
            "  (d)-[:TYPE1 {weight: 2.0}]->(b),\n" +

            "  (e)-[:TYPE1 {weight: 2.0}]->(b),\n" +
            "  (e)-[:TYPE1 {weight: 2.0}]->(d),\n" +
            "  (e)-[:TYPE1 {weight: 2.0}]->(f),\n" +

            "  (f)-[:TYPE1 {weight: 2.0}]->(b),\n" +
            "  (f)-[:TYPE1 {weight: 2.0}]->(e),\n" +

            "  (a)-[:TYPE3 {weight: -2.0}]->(b),\n" +

            "  (b)-[:TYPE3 {weight: 2.0}]->(c),\n" +
            "  (c)-[:TYPE3 {weight: 2.0}]->(b),\n" +

            "  (d)-[:TYPE3 {weight: 2.0}]->(a),\n" +
            "  (d)-[:TYPE3 {weight: 2.0}]->(b),\n" +

            "  (e)-[:TYPE3 {weight: 2.0}]->(b),\n" +
            "  (e)-[:TYPE3 {weight: 2.0}]->(d),\n" +
            "  (e)-[:TYPE3 {weight: 2.0}]->(f),\n" +

            "  (f)-[:TYPE3 {weight: 2.0}]->(b),\n" +
            "  (f)-[:TYPE3 {weight: 2.0}]->(e),\n" +

            "  (g)-[:TYPE2]->(b),\n" +
            "  (g)-[:TYPE2]->(e),\n" +
            "  (h)-[:TYPE2]->(b),\n" +
            "  (h)-[:TYPE2]->(e),\n" +
            "  (i)-[:TYPE2]->(b),\n" +
            "  (i)-[:TYPE2]->(e),\n" +
            "  (j)-[:TYPE2]->(e),\n" +
            "  (k)-[:TYPE2]->(e)\n";

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        if (db!=null) db.shutdown();
    }

    public AverageDegreeCentralityTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void averageOutgoingCentrality() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.0);
            expected.put(db.findNode(label, "name", "b").getId(), 1.0);
            expected.put(db.findNode(label, "name", "c").getId(), 1.0);
            expected.put(db.findNode(label, "name", "d").getId(), 2.0);
            expected.put(db.findNode(label, "name", "e").getId(), 3.0);
            expected.put(db.findNode(label, "name", "f").getId(), 2.0);
            expected.put(db.findNode(label, "name", "g").getId(), 0.0);
            expected.put(db.findNode(label, "name", "h").getId(), 0.0);
            expected.put(db.findNode(label, "name", "i").getId(), 0.0);
            expected.put(db.findNode(label, "name", "j").getId(), 0.0);
            tx.close();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target")
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);
        }

        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(graph, Pools.DEFAULT, 4, Direction.OUTGOING);
        degreeCentrality.compute();

        assertEquals(0.9, degreeCentrality.average(), 0.01);
    }

    @Test
    public void averageIncomingCentrality() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 1.0);
            expected.put(db.findNode(label, "name", "b").getId(), 4.0);
            expected.put(db.findNode(label, "name", "c").getId(), 1.0);
            expected.put(db.findNode(label, "name", "d").getId(), 1.0);
            expected.put(db.findNode(label, "name", "e").getId(), 1.0);
            expected.put(db.findNode(label, "name", "f").getId(), 1.0);
            expected.put(db.findNode(label, "name", "g").getId(), 0.0);
            expected.put(db.findNode(label, "name", "h").getId(), 0.0);
            expected.put(db.findNode(label, "name", "i").getId(), 0.0);
            expected.put(db.findNode(label, "name", "j").getId(), 0.0);
            tx.close();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)<-[:TYPE1]-(m:Label1) RETURN id(n) as source,id(m) as target")
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.INCOMING)
                    .load(graphImpl);
        }

        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(graph, Pools.DEFAULT, 4, Direction.INCOMING);
        degreeCentrality.compute();

        assertEquals(0.9, degreeCentrality.average(), 0.01);
    }

    @Test
    public void totalCentrality() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        // if there are 2 relationships between a pair of nodes these get squashed into a single relationship
        // when we use an undirected graph
        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 1.0);
            expected.put(db.findNode(label, "name", "b").getId(), 4.0);
            expected.put(db.findNode(label, "name", "c").getId(), 1.0);
            expected.put(db.findNode(label, "name", "d").getId(), 3.0);
            expected.put(db.findNode(label, "name", "e").getId(), 3.0);
            expected.put(db.findNode(label, "name", "f").getId(), 2.0);
            expected.put(db.findNode(label, "name", "g").getId(), 0.0);
            expected.put(db.findNode(label, "name", "h").getId(), 0.0);
            expected.put(db.findNode(label, "name", "i").getId(), 0.0);
            expected.put(db.findNode(label, "name", "j").getId(), 0.0);
            tx.close();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]-(m:Label1) RETURN id(n) as source,id(m) as target")
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .asUndirected(true)
                    .load(graphImpl);
        }

        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(graph, Pools.DEFAULT, 4, Direction.OUTGOING);
        degreeCentrality.compute();

        assertEquals(1.4, degreeCentrality.average(), 0.01);
    }
}
