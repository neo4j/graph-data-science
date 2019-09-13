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
import org.neo4j.graphalgo.core.DeduplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
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
                new Object[]{CypherGraphFactory.class, "CypherGraphFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"}
        );
    }

    private static final String DB_CYPHER = "CREATE " +
            "  (_:Label0 {name:\"_\"})\n" +
            ", (a:Label1 {name:\"a\"})\n" +
            ", (b:Label1 {name:\"b\"})\n" +
            ", (c:Label1 {name:\"c\"})\n" +
            ", (d:Label1 {name:\"d\"})\n" +
            ", (e:Label1 {name:\"e\"})\n" +
            ", (f:Label1 {name:\"f\"})\n" +
            ", (g:Label1 {name:\"g\"})\n" +
            ", (h:Label1 {name:\"h\"})\n" +
            ", (i:Label1 {name:\"i\"})\n" +
            ", (j:Label1 {name:\"j\"})\n" +
            ", (k:Label2 {name:\"k\"})\n" +
            ", (l:Label2 {name:\"l\"})\n" +
            ", (m:Label2 {name:\"m\"})\n" +
            ", (n:Label2 {name:\"n\"})\n" +
            ", (o:Label2 {name:\"o\"})\n" +
            ", (p:Label2 {name:\"p\"})\n" +
            ", (q:Label2 {name:\"q\"})\n" +
            ", (r:Label2 {name:\"r\"})\n" +
            ", (s:Label2 {name:\"s\"})\n" +
            ", (t:Label2 {name:\"t\"})\n" +

            ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (d)-[:TYPE1 {weight: 2.0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 2.0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 2.0}]->(f)" +

            ", (f)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (f)-[:TYPE1 {weight: 2.0}]->(e)" +

            ", (a)-[:TYPE3 {weight: -2.0}]->(b)" +

            ", (b)-[:TYPE3 {weight: 2.0}]->(c)" +
            ", (c)-[:TYPE3 {weight: 2.0}]->(b)" +

            ", (d)-[:TYPE3 {weight: 2.0}]->(a)" +
            ", (d)-[:TYPE3 {weight: 2.0}]->(b)" +

            ", (e)-[:TYPE3 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE3 {weight: 2.0}]->(d)" +
            ", (e)-[:TYPE3 {weight: 2.0}]->(f)" +

            ", (f)-[:TYPE3 {weight: 2.0}]->(b)" +
            ", (f)-[:TYPE3 {weight: 2.0}]->(e)" +

            ", (g)-[:TYPE2]->(b)" +
            ", (g)-[:TYPE2]->(e)" +
            ", (h)-[:TYPE2]->(b)" +
            ", (h)-[:TYPE2]->(e)" +
            ", (i)-[:TYPE2]->(b)" +
            ", (i)-[:TYPE2]->(e)" +
            ", (j)-[:TYPE2]->(e)" +
            ", (k)-[:TYPE2]->(e)";

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraphDb() {
        if (db!=null) db.shutdown();
    }

    public DegreeCentralityTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void outgoingCentrality() {
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
        if (graphImpl.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) AS id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) AS source, id(m) AS target")
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
    public void weightedOutgoingCentrality() {
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
        if (graphImpl.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) AS id")
                    .withRelationshipType("MATCH (n:Label1)-[type:TYPE1]->(m:Label1) RETURN id(n) AS source, id(m) AS target, type.weight AS weight")
                    .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);
        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                    .load(graphImpl);
        }

        WeightedDegreeCentrality degreeCentrality = new WeightedDegreeCentrality(graph, Pools.DEFAULT, 1, AllocationTracker.EMPTY);
        degreeCentrality.compute(false);

        IntStream.range(0, expected.size()).forEach(i -> {
            long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    degreeCentrality.degrees().get(i),
                    1e-2
            );
        });
    }

    @Test
    public void excludeNegativeWeights() {
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
        if (graphImpl.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) AS id")
                    .withRelationshipType("MATCH (n:Label1)-[type:TYPE3]->(m:Label1) RETURN id(n) AS source, id(m) AS target, type.weight AS weight")
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

        WeightedDegreeCentrality degreeCentrality = new WeightedDegreeCentrality(graph, Pools.DEFAULT, 1,AllocationTracker.EMPTY);
        degreeCentrality.compute(false);

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    degreeCentrality.degrees().get(i),
                    1e-2
            );
        });
    }

    @Test
    public void incomingCentrality() {
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
        if (graphImpl.isAssignableFrom(CypherGraphFactory.class)) {
            // For Cypher we always treat the graph as outgoing, and let the user
            // handle the direction in the Cypher query
            direction = Direction.OUTGOING;

            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) AS id")
                    .withRelationshipType("MATCH (n:Label1)<-[:TYPE1]-(m:Label1) RETURN id(n) AS source, id(m) AS target")
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
    public void weightedIncomingCentrality() {
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

        Graph graph;
        if (graphImpl.isAssignableFrom(CypherGraphFactory.class)) {
            // For Cypher we always treat the graph as outgoing, and let the user
            // handle the direction in the Cypher query
            direction = Direction.OUTGOING;


            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) AS id")
                    .withRelationshipType("MATCH (n:Label1)<-[t:TYPE1]-(m:Label1) RETURN id(n) AS source, id(m) AS target, t.weight AS weight")
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

        WeightedDegreeCentrality degreeCentrality = new WeightedDegreeCentrality(graph, Pools.DEFAULT, 4, AllocationTracker.EMPTY);
        degreeCentrality.compute(false);

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    degreeCentrality.degrees().get(i),
                    1e-2
            );
        });
    }

    @Test
    public void totalCentrality() {
        Label label = Label.label("Label1");
        Map<Long, Double> expected = new HashMap<>();

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
        if (graphImpl.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) AS id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]-(m:Label1) RETURN id(n) AS source, id(m) AS target")
                    .withDeduplicateRelationshipsStrategy(DeduplicateRelationshipsStrategy.SKIP)
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
            long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId + "[" + i + "]",
                    expected.get(nodeId),
                    degreeCentrality.result().score(i),
                    1e-2
            );
        });
    }
}
