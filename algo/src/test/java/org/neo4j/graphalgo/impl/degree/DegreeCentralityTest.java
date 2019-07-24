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
package org.neo4j.graphalgo.impl.degree;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class DegreeCentralityTest {

    private Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{HeavyCypherGraphFactory.class, "HeavyCypherGraphFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"}
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

    public DegreeCentralityTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void outgoingCentrality() throws Exception {
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
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target")
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);
        }

        DegreeCentrality degreeCentrality = new DegreeCentrality(graph, Pools.DEFAULT, 4, Direction.OUTGOING, false);
        degreeCentrality.compute();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    degreeCentrality.result().score(i),
                    1e-2
            );
        });
    }

    @Test
    public void weightedOutgoingCentrality() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.0);
            expected.put(db.findNode(label, "name", "b").getId(), 2.0);
            expected.put(db.findNode(label, "name", "c").getId(), 2.0);
            expected.put(db.findNode(label, "name", "d").getId(), 4.0);
            expected.put(db.findNode(label, "name", "e").getId(), 6.0);
            expected.put(db.findNode(label, "name", "f").getId(), 4.0);
            expected.put(db.findNode(label, "name", "g").getId(), 0.0);
            expected.put(db.findNode(label, "name", "h").getId(), 0.0);
            expected.put(db.findNode(label, "name", "i").getId(), 0.0);
            expected.put(db.findNode(label, "name", "j").getId(), 0.0);
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[type:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target, type.weight AS weight")
                    .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                    .load(graphImpl);
        }

        WeightedDegreeCentrality degreeCentrality = new WeightedDegreeCentrality(graph, Pools.DEFAULT, 1);
        degreeCentrality.compute(false);

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    degreeCentrality.degrees()[i],
                    1e-2
            );
        });
    }

    @Test
    public void excludeNegativeWeights() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.0);
            expected.put(db.findNode(label, "name", "b").getId(), 2.0);
            expected.put(db.findNode(label, "name", "c").getId(), 2.0);
            expected.put(db.findNode(label, "name", "d").getId(), 4.0);
            expected.put(db.findNode(label, "name", "e").getId(), 6.0);
            expected.put(db.findNode(label, "name", "f").getId(), 4.0);
            expected.put(db.findNode(label, "name", "g").getId(), 0.0);
            expected.put(db.findNode(label, "name", "h").getId(), 0.0);
            expected.put(db.findNode(label, "name", "i").getId(), 0.0);
            expected.put(db.findNode(label, "name", "j").getId(), 0.0);
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[type:TYPE3]->(m:Label1) RETURN id(n) as source,id(m) as target, type.weight AS weight")
                    .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE3")
                    .withDirection(Direction.OUTGOING)
                    .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                    .load(graphImpl);
        }

        WeightedDegreeCentrality degreeCentrality = new WeightedDegreeCentrality(graph, Pools.DEFAULT, 1);
        degreeCentrality.compute(false);

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    degreeCentrality.degrees()[i],
                    1e-2
            );
        });
    }

    @Test
    public void incomingCentrality() throws Exception {
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
        }

        Direction direction = Direction.INCOMING;

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            // For Cypher we always treat the graph as outgoing, and let the user
            // handle the direction in the Cypher query
            direction = Direction.OUTGOING;

            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)<-[:TYPE1]-(m:Label1) RETURN id(n) as source,id(m) as target")
                    .withDirection(direction)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(direction)
                    .load(graphImpl);
        }

        DegreeCentrality degreeCentrality = new DegreeCentrality(graph, Pools.DEFAULT, 4, direction, false);
        degreeCentrality.compute();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    degreeCentrality.result().score(i),
                    1e-2
            );
        });
    }

    @Test
    public void weightedIncomingCentrality() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 2.0);
            expected.put(db.findNode(label, "name", "b").getId(), 8.0);
            expected.put(db.findNode(label, "name", "c").getId(), 2.0);
            expected.put(db.findNode(label, "name", "d").getId(), 2.0);
            expected.put(db.findNode(label, "name", "e").getId(), 2.0);
            expected.put(db.findNode(label, "name", "f").getId(), 2.0);
            expected.put(db.findNode(label, "name", "g").getId(), 0.0);
            expected.put(db.findNode(label, "name", "h").getId(), 0.0);
            expected.put(db.findNode(label, "name", "i").getId(), 0.0);
            expected.put(db.findNode(label, "name", "j").getId(), 0.0);
        }

        Direction direction = Direction.INCOMING;

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            // For Cypher we always treat the graph as outgoing, and let the user
            // handle the direction in the Cypher query
            direction = Direction.OUTGOING;


            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)<-[t:TYPE1]-(m:Label1) RETURN id(n) as source,id(m) as target, t.weight AS weight")
                    .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                    .withDirection(direction)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                    .withDirection(direction)
                    .load(graphImpl);
        }

        WeightedDegreeCentrality degreeCentrality = new WeightedDegreeCentrality(graph, Pools.DEFAULT, 4);
        degreeCentrality.compute(false);

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    degreeCentrality.degrees()[i],
                    1e-2
            );
        });
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
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]-(m:Label1) RETURN id(n) as source,id(m) as target")
                    .withDuplicateRelationshipsStrategy(DuplicateRelationshipsStrategy.SKIP)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .undirected()
                    .load(graphImpl);
        }

        DegreeCentrality degreeCentrality = new DegreeCentrality(graph, Pools.DEFAULT, 4, Direction.OUTGOING, false);
        degreeCentrality.compute();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId + "[" + i + "]",
                    expected.get(nodeId),
                    degreeCentrality.result().score(i),
                    1e-2
            );
        });
    }
}
