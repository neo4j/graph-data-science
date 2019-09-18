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
package org.neo4j.graphalgo.impl.pagerank;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.impl.pagerank.PageRankTest.DEFAULT_CONFIG;

final class WeightedPageRankTest {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (_:Label0 {name: '_'})" +
            ", (a:Label1 {name: 'a'})" +
            ", (b:Label1 {name: 'b'})" +
            ", (c:Label1 {name: 'c'})" +
            ", (d:Label1 {name: 'd'})" +
            ", (e:Label1 {name: 'e'})" +
            ", (f:Label1 {name: 'f'})" +
            ", (g:Label1 {name: 'g'})" +
            ", (h:Label1 {name: 'h'})" +
            ", (i:Label1 {name: 'i'})" +
            ", (j:Label1 {name: 'j'})" +
            ", (k:Label2 {name: 'k'})" +
            ", (l:Label2 {name: 'l'})" +
            ", (m:Label2 {name: 'm'})" +
            ", (n:Label2 {name: 'n'})" +
            ", (o:Label2 {name: 'o'})" +
            ", (p:Label2 {name: 'p'})" +
            ", (q:Label2 {name: 'q'})" +
            ", (r:Label2 {name: 'r'})" +
            ", (s:Label2 {name: 's'})" +
            ", (t:Label2 {name: 't'})" +

            ", (b)-[:TYPE1]->(c)" +
            ", (c)-[:TYPE1]->(b)" +
            ", (d)-[:TYPE1]->(a)" +
            ", (d)-[:TYPE1]->(b)" +
            ", (e)-[:TYPE1]->(b)" +
            ", (e)-[:TYPE1]->(d)" +
            ", (e)-[:TYPE1]->(f)" +
            ", (f)-[:TYPE1]->(b)" +
            ", (f)-[:TYPE1]->(e)" +

            ", (b)-[:TYPE2 {weight: 1}]->(c)" +
            ", (c)-[:TYPE2 {weight: 1}]->(b)" +
            ", (d)-[:TYPE2 {weight: 1}]->(a)" +
            ", (d)-[:TYPE2 {weight: 1}]->(b)" +
            ", (e)-[:TYPE2 {weight: 1}]->(b)" +
            ", (e)-[:TYPE2 {weight: 1}]->(d)" +
            ", (e)-[:TYPE2 {weight: 1}]->(f)" +
            ", (f)-[:TYPE2 {weight: 1}]->(b)" +
            ", (f)-[:TYPE2 {weight: 1}]->(e)" +

            ", (b)-[:TYPE3 {weight: 1.0}]->(c)" +
            ", (c)-[:TYPE3 {weight: 1.0}]->(b)" +
            ", (d)-[:TYPE3 {weight: 0.3}]->(a)" +
            ", (d)-[:TYPE3 {weight: 0.7}]->(b)" +
            ", (e)-[:TYPE3 {weight: 0.9}]->(b)" +
            ", (e)-[:TYPE3 {weight: 0.05}]->(d)" +
            ", (e)-[:TYPE3 {weight: 0.05}]->(f)" +
            ", (f)-[:TYPE3 {weight: 0.9}]->(b)" +
            ", (f)-[:TYPE3 {weight: 0.1}]->(e)" +

            ", (b)-[:TYPE4 {weight: 1.0}]->(c)" +
            ", (c)-[:TYPE4 {weight: 1.0}]->(b)" +
            ", (d)-[:TYPE4 {weight: 0.3}]->(a)" +
            ", (d)-[:TYPE4 {weight: 0.7}]->(b)" +
            ", (e)-[:TYPE4 {weight: 0.9}]->(b)" +
            ", (e)-[:TYPE4 {weight: 0.05}]->(d)" +
            ", (e)-[:TYPE4 {weight: 0.05}]->(f)" +
            ", (f)-[:TYPE4 {weight: 0.9}]->(b)" +
            ", (f)-[:TYPE4 {weight: -0.9}]->(a)" +
            ", (f)-[:TYPE4 {weight: 0.1}]->(e)";

    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setupGraphDb() {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.execute(DB_CYPHER);
    }

    @AfterAll
    static void shutdownGraphDb() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphTypesTest
    void defaultWeightOf0MeansNoDiffusionOfPageRank(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = DB.beginTx()) {
            expected.put(DB.findNode(label, "name", "a").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "b").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "c").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "d").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "e").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "f").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "g").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "h").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "i").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "j").getId(), 0.15);
            tx.success();
        }

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(DB)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(DB)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .withDirection(Direction.OUTGOING)
                    .load(graphFactory);
        }

        final CentralityResult rankResult = PageRankAlgorithmType.WEIGHTED
                .create(graph, DEFAULT_CONFIG, LongStream.empty())
                .compute()
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

    @AllGraphTypesTest
    void defaultWeightOf1ShouldBeTheSameAsPageRank(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = DB.beginTx()) {
            expected.put(DB.findNode(label, "name", "a").getId(), 0.243007);
            expected.put(DB.findNode(label, "name", "b").getId(), 1.9183995);
            expected.put(DB.findNode(label, "name", "c").getId(), 1.7806315);
            expected.put(DB.findNode(label, "name", "d").getId(), 0.21885);
            expected.put(DB.findNode(label, "name", "e").getId(), 0.243007);
            expected.put(DB.findNode(label, "name", "f").getId(), 0.21885);
            expected.put(DB.findNode(label, "name", "g").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "h").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "i").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "j").getId(), 0.15);
            tx.success();
        }

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(DB)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n:Label1)-[:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target")
                    .withRelationshipWeightsFromProperty("weight", 1)
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(DB)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withRelationshipWeightsFromProperty("weight", 1)
                    .withDirection(Direction.OUTGOING)
                    .load(graphFactory);
        }

        final CentralityResult rankResult = PageRankAlgorithmType.WEIGHTED
                .create(graph, DEFAULT_CONFIG, LongStream.empty())
                .compute()
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

    @AllGraphTypesTest
    void allWeightsTheSameShouldBeTheSameAsPageRank(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = DB.beginTx()) {
            expected.put(DB.findNode(label, "name", "a").getId(), 0.243007);
            expected.put(DB.findNode(label, "name", "b").getId(), 1.9183995);
            expected.put(DB.findNode(label, "name", "c").getId(), 1.7806315);
            expected.put(DB.findNode(label, "name", "d").getId(), 0.21885);
            expected.put(DB.findNode(label, "name", "e").getId(), 0.243007);
            expected.put(DB.findNode(label, "name", "f").getId(), 0.21885);
            expected.put(DB.findNode(label, "name", "g").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "h").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "i").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "j").getId(), 0.15);
            tx.success();
        }

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(DB)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType(
                            "MATCH (n:Label1)-[r:TYPE2]->(m:Label1) RETURN id(n) as source,id(m) as target, r.weight AS weight")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(DB)
                    .withLabel(label)
                    .withRelationshipType("TYPE2")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .withDirection(Direction.OUTGOING)
                    .load(graphFactory);
        }

        final CentralityResult rankResult = PageRankAlgorithmType.WEIGHTED
                .create(graph, DEFAULT_CONFIG, LongStream.empty())
                .compute()
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

    @AllGraphTypesTest
    void higherWeightsLeadToHigherPageRank(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = DB.beginTx()) {
            expected.put(DB.findNode(label, "name", "a").getId(), 0.1900095);
            expected.put(DB.findNode(label, "name", "b").getId(), 2.2152279);
            expected.put(DB.findNode(label, "name", "c").getId(), 2.0325884);
            expected.put(DB.findNode(label, "name", "d").getId(), 0.1569275);
            expected.put(DB.findNode(label, "name", "e").getId(), 0.1633280);
            expected.put(DB.findNode(label, "name", "f").getId(), 0.1569275);
            expected.put(DB.findNode(label, "name", "g").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "h").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "i").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "j").getId(), 0.15);
            tx.success();
        }

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(DB)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType(
                            "MATCH (n:Label1)-[r:TYPE3]->(m:Label1) RETURN id(n) as source,id(m) as target, r.weight AS weight")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(DB)
                    .withLabel(label)
                    .withRelationshipType("TYPE3")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .withDirection(Direction.OUTGOING)
                    .load(graphFactory);
        }

        final CentralityResult rankResult = PageRankAlgorithmType.WEIGHTED
                .create(graph, DEFAULT_CONFIG, LongStream.empty())
                .compute()
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

    @AllGraphTypesTest
    void shouldExcludeNegativeWeights(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = DB.beginTx()) {
            expected.put(DB.findNode(label, "name", "a").getId(), 0.1900095);
            expected.put(DB.findNode(label, "name", "b").getId(), 2.2152279);
            expected.put(DB.findNode(label, "name", "c").getId(), 2.0325884);
            expected.put(DB.findNode(label, "name", "d").getId(), 0.1569275);
            expected.put(DB.findNode(label, "name", "e").getId(), 0.1633280);
            expected.put(DB.findNode(label, "name", "f").getId(), 0.1569275);
            expected.put(DB.findNode(label, "name", "g").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "h").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "i").getId(), 0.15);
            expected.put(DB.findNode(label, "name", "j").getId(), 0.15);
            tx.success();
        }

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(DB)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType(
                            "MATCH (n:Label1)-[r:TYPE4]->(m:Label1) RETURN id(n) as source,id(m) as target, r.weight AS weight")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(DB)
                    .withLabel(label)
                    .withRelationshipType("TYPE4")
                    .withRelationshipWeightsFromProperty("weight", 0)
                    .withDirection(Direction.OUTGOING)
                    .load(graphFactory);
        }

        final CentralityResult rankResult = PageRankAlgorithmType.WEIGHTED
                .create(graph, DEFAULT_CONFIG, LongStream.empty())
                .compute()
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
