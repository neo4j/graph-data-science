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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestGraphLoader;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class CypherGraphFactoryTest {

    private static final int COUNT = 10_000;
    private static final String DB_CYPHER = "UNWIND range(1, $count) AS id " +
                                            "CREATE (n {id: id})-[:REL {prop: id % 10}]->(n)";
    private static final String SKIP_LIMIT = "WITH * SKIP $skip LIMIT $limit";

    private GraphDatabaseAPI db;

    private static int id1;
    private static int id2;
    private static int id3;

    @BeforeEach
    void setUp() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db, DB_CYPHER, MapUtil.map("count", COUNT));
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void testLoadCypher() {
        db = TestDatabaseCreator.createTestDatabase();

        String query = " CREATE (n1 {partition: 6})-[:REL {prop: 1}]->(n2 {foo: 4})-[:REL {prop: 2}]->(n3)" +
                       " CREATE (n1)-[:REL {prop: 3}]->(n3)"+
                       " RETURN id(n1) AS id1, id(n2) AS id2, id(n3) AS id3";
        runQuery(db, query).accept( row -> {
            id1 = row.getNumber("id1").intValue();
            id2 = row.getNumber("id2").intValue();
            id3 = row.getNumber("id3").intValue();
            return true;
        });
        String nodes = "MATCH (n) RETURN id(n) AS id, n.partition AS partition, n.foo AS foo";
        String rels = "MATCH (n)-[r]->(m) WHERE type(r) = $rel RETURN id(n) AS source, id(m) AS target, r.prop AS weight ORDER BY id(r) DESC ";

        Graph graph = runInTransaction(
            db,
            () -> new GraphLoader(db)
                .withParams(MapUtil.map("rel", "REL"))
                .withRelationshipProperties(PropertyMapping.of("weight", 0))
                .withLabel(nodes)
                .withRelationshipType(rels)
                .withOptionalNodeProperties(
                    PropertyMapping.of("partition", "partition", 0.0),
                    PropertyMapping.of("foo", "foo", 5.0)
                )
                .sorted()
                .load(CypherGraphFactory.class)
        );

        long node1 = graph.toMappedNodeId(id1);
        long node2 = graph.toMappedNodeId(id2);
        long node3 = graph.toMappedNodeId(id3);

        assertEquals(3, graph.nodeCount());
        assertEquals(2, graph.degree(node1, Direction.OUTGOING));
        assertEquals(1, graph.degree(node2, Direction.OUTGOING));
        assertEquals(0, graph.degree(node3, Direction.OUTGOING));
        AtomicInteger total = new AtomicInteger();
        graph.forEachNode(n -> {
            graph.forEachRelationship(n, Direction.OUTGOING, Double.NaN, (s, t, w) -> {
                String rel = "(" + s + ")-->(" + t + ")";
                if (s == id1 && t == id2) {
                    assertEquals(1.0, w, "weight of " + rel);
                } else if (s == id2 && t == id3) {
                    assertEquals(2.0, w, "weight of " + rel);
                } else if (s == id1 && t == id3) {
                    assertEquals(3.0, w, "weight of " + rel);
                } else {
                    fail("Unexpected relationship " + rel);
                }
                total.addAndGet((int) w);
                return true;
            });
            return true;
        });
        assertEquals(6, total.get());

        assertEquals(6.0D, graph.nodeProperties("partition").nodeProperty(node1), 0.01);
        assertEquals(5.0D, graph.nodeProperties("foo").nodeProperty(node1), 0.01);
        assertEquals(4.0D, graph.nodeProperties("foo").nodeProperty(node2), 0.01);
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {true, false})
    void testLoadNoneParallelCypher(boolean parallel) {
        String nodeStatement = "MATCH (n) RETURN id(n) AS id";
        String relStatement = "MATCH (n)-[r:REL]->(m) RETURN id(n) AS source, id(m) AS target, r.prop AS weight";

        loadAndTestGraph(nodeStatement, relStatement, DeduplicationStrategy.NONE, parallel);
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {true, false})
    void testLoadNodesParallelCypher(boolean parallel) {
        String pagingQuery = "MATCH (n) %s RETURN id(n) AS id";
        String nodeStatement = String.format(pagingQuery, parallel ? SKIP_LIMIT : "");
        String relStatement = "MATCH (n)-[r:REL]->(m) RETURN id(n) AS source, id(m) AS target, r.prop AS weight";

        loadAndTestGraph(nodeStatement, relStatement, DeduplicationStrategy.NONE, parallel);
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {true, false})
    void testLoadRelationshipsParallelCypher(boolean parallel) {
        String nodeStatement = "MATCH (n) RETURN id(n) AS id";
        String pagingQuery = "MATCH (n)-[r:REL]->(m) %s RETURN id(n) AS source, id(m) AS target, r.prop AS weight";
        String relStatement = String.format(pagingQuery, parallel ? SKIP_LIMIT : "");

        loadAndTestGraph(nodeStatement, relStatement, DeduplicationStrategy.NONE, parallel);
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {true, false})
    void testLoadRelationshipsParallelAccumulateWeightCypher(boolean parallel) {
        String nodeStatement = "MATCH (n) RETURN id(n) AS id";
        String pagingQuery =
                "MATCH (n)-[r:REL]->(m) %1$s RETURN id(n) AS source, id(m) AS target, r.prop/2.0 AS weight " +
                "UNION ALL "+
                "MATCH (n)-[r:REL]->(m) %1$s RETURN id(n) AS source, id(m) AS target, r.prop/2.0 AS weight ";
        String relStatement = String.format(pagingQuery, parallel ? SKIP_LIMIT : "");

        loadAndTestGraph(nodeStatement, relStatement, DeduplicationStrategy.SUM, parallel);
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {true, false})
    void testLoadCypherBothParallel(boolean parallel) {
        String pagingNodeQuery = "MATCH (n) %s RETURN id(n) AS id";
        String nodeStatement = String.format(pagingNodeQuery, parallel ? SKIP_LIMIT : "");
        String pagingRelQuery = "MATCH (n)-[r:REL]->(m) %s RETURN id(n) AS source, id(m) AS target, r.prop AS weight";
        String relStatement = String.format(pagingRelQuery, parallel ? SKIP_LIMIT : "");

        loadAndTestGraph(nodeStatement, relStatement, DeduplicationStrategy.NONE, parallel);
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {true, false})
    void uniqueRelationships(boolean parallel) {
        String nodeStatement = "MATCH (n) RETURN id(n) AS id";
        String pagingQuery = "MATCH (n)-[r:REL]->(m) %s RETURN id(n) AS source, id(m) AS target, r.prop AS weight";
        String relStatement = String.format(pagingQuery, parallel ? SKIP_LIMIT : "");

        loadAndTestGraph(nodeStatement, relStatement, DeduplicationStrategy.NONE, parallel);
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {true, false})
    void accumulateWeightCypher(boolean parallel) {
        String nodeStatement = "MATCH (n) RETURN id(n) AS id";
        String pagingQuery =
                "MATCH (n)-[r:REL]->(m) %1$s RETURN id(n) AS source, id(m) AS target, r.prop/2.0 AS weight " +
                "UNION ALL "+
                "MATCH (n)-[r:REL]->(m) %1$s RETURN id(n) AS source, id(m) AS target, r.prop/2.0 AS weight ";
        String relStatement = String.format(pagingQuery, parallel ? SKIP_LIMIT : "");

        loadAndTestGraph(nodeStatement, relStatement, DeduplicationStrategy.SUM, parallel);
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {true, false})
    void countEachRelationshipOnce(boolean parallel) {
        String nodeStatement = "MATCH (n) RETURN id(n) AS id";
        String pagingQuery =
                "MATCH (n)-[r:REL]->(m) %1$s RETURN id(n) AS source, id(m) AS target, r.prop AS weight " +
                "UNION ALL "+
                "MATCH (n)-[r:REL]->(m) %1$s RETURN id(n) AS source, id(m) AS target, r.prop AS weight ";
        String relStatement = String.format(pagingQuery, parallel ? SKIP_LIMIT : "");

        loadAndTestGraph(nodeStatement, relStatement, DeduplicationStrategy.SINGLE, parallel);
    }

    @Test
    void testInitNodePropertiesFromQuery() {
        GraphDatabaseAPI db = TestDatabaseCreator.createTestDatabase();
        runQuery(
            db,
            "CREATE" +
            "  (n1:Node1 {prop1: 1})" +
            ", (n2:Node2 {prop2: 2})" +
            ", (n3:Node3 {prop3: 3})"
        );
        PropertyMapping prop1Mapping = PropertyMapping.of("prop1", 0D);
        PropertyMapping prop2Mapping = PropertyMapping.of("prop2", 0D);
        PropertyMapping prop3Mapping = PropertyMapping.of("prop3", 0D);
        PropertyMappings nodePropertyMappings = PropertyMappings.of(prop1Mapping, prop2Mapping, prop3Mapping);

        Graph graph = TestGraphLoader
            .from(db)
            .withNodeProperties(nodePropertyMappings, false)
            .buildGraph(CypherGraphFactory.class);

        String prop1Key = TestGraphLoader.addSuffix(prop1Mapping.propertyKey(), 0);
        String prop2Key = TestGraphLoader.addSuffix(prop2Mapping.propertyKey(), 1);
        String prop3Key = TestGraphLoader.addSuffix(prop3Mapping.propertyKey(), 2);

        String gdl = "(a {prop1: 1, prop2: 0, prop3: 0})" +
                     "(b {prop1: 0, prop2: 2, prop3: 0})" +
                     "(c {prop1: 0, prop2: 0, prop3: 3})" +
                     "(a)-->(b), (a)-->(c), (b)-->(c)";

        String expectedGdl = gdl
            .replaceAll(prop1Mapping.propertyKey(), prop1Key)
            .replaceAll(prop2Mapping.propertyKey(), prop2Key)
            .replaceAll(prop3Mapping.propertyKey(), prop3Key);

        assertGraphEquals(fromGdl(expectedGdl), graph);
    }


    private void loadAndTestGraph(String nodeStatement, String relStatement, DeduplicationStrategy strategy, boolean parallel) {
        GraphLoader loader = new GraphLoader(db)
                .withBatchSize(1000)
                .withDeduplicationStrategy(strategy)
                .withRelationshipProperties(PropertyMapping.of("weight", 0D))
                .withLabel(nodeStatement)
                .withRelationshipType(relStatement);
        if (parallel) {
            loader.withExecutorService(Pools.DEFAULT);
        }
        Graph graph = runInTransaction(db, () -> loader.load(CypherGraphFactory.class));

        assertEquals(COUNT, graph.nodeCount());
        AtomicInteger relCount = new AtomicInteger();
        graph.forEachNode(node -> {
            relCount.addAndGet(graph.degree(node, Direction.OUTGOING));
            return true;
        });
        assertEquals(COUNT, relCount.get());
        AtomicInteger total = new AtomicInteger();
        graph.forEachNode(n -> {
            graph.forEachRelationship(n, Direction.OUTGOING, Double.NaN, (s, t, w) -> {
                total.addAndGet((int) w);
                return true;
            });
            return true;
        });
        assertEquals(9 * COUNT / 2, total.get());
    }
}
