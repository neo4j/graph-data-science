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
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.impl.pagerank.PageRankFactory;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class WeightedPageRankTest {
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
            "  (b)-[:TYPE1]->(c),\n" +
            "  (c)-[:TYPE1]->(b),\n" +
            "  (d)-[:TYPE1]->(a),\n" +
            "  (d)-[:TYPE1]->(b),\n" +
            "  (e)-[:TYPE1]->(b),\n" +
            "  (e)-[:TYPE1]->(d),\n" +
            "  (e)-[:TYPE1]->(f),\n" +
            "  (f)-[:TYPE1]->(b),\n" +
            "  (f)-[:TYPE1]->(e),\n" +

            "  (b)-[:TYPE2 {weight: 1}]->(c),\n" +
            "  (c)-[:TYPE2 {weight: 1}]->(b),\n" +
            "  (d)-[:TYPE2 {weight: 1}]->(a),\n" +
            "  (d)-[:TYPE2 {weight: 1}]->(b),\n" +
            "  (e)-[:TYPE2 {weight: 1}]->(b),\n" +
            "  (e)-[:TYPE2 {weight: 1}]->(d),\n" +
            "  (e)-[:TYPE2 {weight: 1}]->(f),\n" +
            "  (f)-[:TYPE2 {weight: 1}]->(b),\n" +
            "  (f)-[:TYPE2 {weight: 1}]->(e),\n" +

            "  (b)-[:TYPE3 {weight: 1.0}]->(c),\n" +
            "  (c)-[:TYPE3 {weight: 1.0}]->(b),\n" +
            "  (d)-[:TYPE3 {weight: 0.3}]->(a),\n" +
            "  (d)-[:TYPE3 {weight: 0.7}]->(b),\n" +
            "  (e)-[:TYPE3 {weight: 0.9}]->(b),\n" +
            "  (e)-[:TYPE3 {weight: 0.05}]->(d),\n" +
            "  (e)-[:TYPE3 {weight: 0.05}]->(f),\n" +
            "  (f)-[:TYPE3 {weight: 0.9}]->(b),\n" +
            "  (f)-[:TYPE3 {weight: 0.1}]->(e),\n" +

            "  (b)-[:TYPE4 {weight: 1.0}]->(c),\n" +
            "  (c)-[:TYPE4 {weight: 1.0}]->(b),\n" +
            "  (d)-[:TYPE4 {weight: 0.3}]->(a),\n" +
            "  (d)-[:TYPE4 {weight: 0.7}]->(b),\n" +
            "  (e)-[:TYPE4 {weight: 0.9}]->(b),\n" +
            "  (e)-[:TYPE4 {weight: 0.05}]->(d),\n" +
            "  (e)-[:TYPE4 {weight: 0.05}]->(f),\n" +
            "  (f)-[:TYPE4 {weight: 0.9}]->(b),\n" +
            "  (f)-[:TYPE4 {weight: -0.9}]->(a),\n" +
            "  (f)-[:TYPE4 {weight: 0.1}]->(e)\n";

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

    public WeightedPageRankTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void defaultWeightOf0MeansNoDiffusionOfPageRank() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.15);
            expected.put(db.findNode(label, "name", "b").getId(), 0.15);
            expected.put(db.findNode(label, "name", "c").getId(), 0.15);
            expected.put(db.findNode(label, "name", "d").getId(), 0.15);
            expected.put(db.findNode(label, "name", "e").getId(), 0.15);
            expected.put(db.findNode(label, "name", "f").getId(), 0.15);
            expected.put(db.findNode(label, "name", "g").getId(), 0.15);
            expected.put(db.findNode(label, "name", "h").getId(), 0.15);
            expected.put(db.findNode(label, "name", "i").getId(), 0.15);
            expected.put(db.findNode(label, "name", "j").getId(), 0.15);
            tx.close();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);
        }

        final CentralityResult rankResult = PageRankFactory
                .weightedOf(graph, 0.85, LongStream.empty())
                .compute(40)
                .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2
            );
        });
    }

    @Test
    public void defaultWeightOf1ShouldBeTheSameAsPageRank() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.243007);
            expected.put(db.findNode(label, "name", "b").getId(), 1.9183995);
            expected.put(db.findNode(label, "name", "c").getId(), 1.7806315);
            expected.put(db.findNode(label, "name", "d").getId(), 0.21885);
            expected.put(db.findNode(label, "name", "e").getId(), 0.243007);
            expected.put(db.findNode(label, "name", "f").getId(), 0.21885);
            expected.put(db.findNode(label, "name", "g").getId(), 0.15);
            expected.put(db.findNode(label, "name", "h").getId(), 0.15);
            expected.put(db.findNode(label, "name", "i").getId(), 0.15);
            expected.put(db.findNode(label, "name", "j").getId(), 0.15);
            tx.close();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target")
                    .withRelationshipWeightsFromProperty("weight", 1)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withRelationshipWeightsFromProperty("weight", 1)
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);
        }

        final CentralityResult rankResult = PageRankFactory
                .weightedOf(graph, 0.85, LongStream.empty())
                .compute(40)
                .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2
            );
        });
    }

    @Test
    public void allWeightsTheSameShouldBeTheSameAsPageRank() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.243007);
            expected.put(db.findNode(label, "name", "b").getId(), 1.9183995);
            expected.put(db.findNode(label, "name", "c").getId(), 1.7806315);
            expected.put(db.findNode(label, "name", "d").getId(), 0.21885);
            expected.put(db.findNode(label, "name", "e").getId(), 0.243007);
            expected.put(db.findNode(label, "name", "f").getId(), 0.21885);
            expected.put(db.findNode(label, "name", "g").getId(), 0.15);
            expected.put(db.findNode(label, "name", "h").getId(), 0.15);
            expected.put(db.findNode(label, "name", "i").getId(), 0.15);
            expected.put(db.findNode(label, "name", "j").getId(), 0.15);
            tx.close();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[r:TYPE2]->(m:Label1) RETURN id(n) as source,id(m) as target, r.weight AS weight")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE2")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);
        }

        final CentralityResult rankResult = PageRankFactory
                .weightedOf(graph, 0.85, LongStream.empty())
                .compute(40)
                .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2
            );
        });
    }

    @Test
    public void higherWeightsLeadToHigherPageRank() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.1900095);
            expected.put(db.findNode(label, "name", "b").getId(), 2.2152279);
            expected.put(db.findNode(label, "name", "c").getId(), 2.0325884);
            expected.put(db.findNode(label, "name", "d").getId(), 0.1569275);
            expected.put(db.findNode(label, "name", "e").getId(), 0.1633280);
            expected.put(db.findNode(label, "name", "f").getId(), 0.1569275);
            expected.put(db.findNode(label, "name", "g").getId(), 0.15);
            expected.put(db.findNode(label, "name", "h").getId(), 0.15);
            expected.put(db.findNode(label, "name", "i").getId(), 0.15);
            expected.put(db.findNode(label, "name", "j").getId(), 0.15);
            tx.close();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[r:TYPE3]->(m:Label1) RETURN id(n) as source,id(m) as target, r.weight AS weight")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE3")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);
        }

        final CentralityResult rankResult = PageRankFactory
                .weightedOf(graph, 0.85, LongStream.empty())
                .compute(40)
                .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2
            );
        });
    }

    @Test
    public void shouldExcludeNegativeWeights() throws Exception {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.1900095);
            expected.put(db.findNode(label, "name", "b").getId(), 2.2152279);
            expected.put(db.findNode(label, "name", "c").getId(), 2.0325884);
            expected.put(db.findNode(label, "name", "d").getId(), 0.1569275);
            expected.put(db.findNode(label, "name", "e").getId(), 0.1633280);
            expected.put(db.findNode(label, "name", "f").getId(), 0.1569275);
            expected.put(db.findNode(label, "name", "g").getId(), 0.15);
            expected.put(db.findNode(label, "name", "h").getId(), 0.15);
            expected.put(db.findNode(label, "name", "i").getId(), 0.15);
            expected.put(db.findNode(label, "name", "j").getId(), 0.15);
            tx.close();
        }

        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[r:TYPE4]->(m:Label1) RETURN id(n) as source,id(m) as target, r.weight AS weight")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withLabel(label)
                    .withRelationshipType("TYPE4")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .withDirection(Direction.OUTGOING)
                    .load(graphImpl);
        }

        final CentralityResult rankResult = PageRankFactory
                .weightedOf(graph, 0.85, LongStream.empty())
                .compute(40)
                .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2
            );
        });
    }
}
