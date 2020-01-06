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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.GraphHelper.collectTargetProperties;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

class CypherGraphFactoryDeduplicationTest {

    public static final String DB_CYPHER = "MERGE (n1 {id: 1})" +
                                           "MERGE (n2 {id: 2}) " +
                                           "CREATE (n1)-[:REL {weight: 4}]->(n2) " +
                                           "CREATE (n2)-[:REL {weight: 10}]->(n1) " +
                                           "RETURN id(n1) AS id1, id(n2) AS id2";

    private GraphDatabaseAPI db;

    private static int id1;
    private static int id2;

    @BeforeEach
    void setUp() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db, DB_CYPHER).accept(row -> {
            id1 = row.getNumber("id1").intValue();
            id2 = row.getNumber("id2").intValue();
            return true;
        });
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void testLoadCypher() {
        String nodes = "MATCH (n) RETURN id(n) AS id";
        String rels = "MATCH (n)-[r]-(m) RETURN id(n) AS source, id(m) AS target, r.weight AS weight";

        Graph graph = runInTransaction(
            db,
            () -> new GraphLoader(db)
                .withLabel(nodes)
                .withRelationshipType(rels)
                .withDeduplicationStrategy(DeduplicationStrategy.SINGLE)
                .load(CypherGraphFactory.class)
        );

        assertEquals(2, graph.nodeCount());
        assertEquals(1, graph.degree(graph.toMappedNodeId(id1), Direction.OUTGOING));
        assertEquals(1, graph.degree(graph.toMappedNodeId(id2), Direction.OUTGOING));
    }

    @Test
    void testLoadCypherDuplicateRelationshipsWithWeights() {
        String nodes = "MATCH (n) RETURN id(n) AS id";
        String rels = "MATCH (n)-[r]-(m) RETURN id(n) AS source, id(m) AS target, r.weight AS weight";

        Graph graph = runInTransaction(
            db,
            () -> new GraphLoader(db)
                .withLabel(nodes)
                .withRelationshipType(rels)
                .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
                .withDeduplicationStrategy(DeduplicationStrategy.SINGLE)
                .load(CypherGraphFactory.class)
        );

        double[] weights = collectTargetProperties(graph, graph.toMappedNodeId(id1));
        assertEquals(1, weights.length);
        assertTrue(weights[0] == 10.0 || weights[0] == 4.0);
    }

    @ParameterizedTest
    @CsvSource({"SUM, 14.0", "MAX, 10.0", "MIN, 4.0"})
    void testLoadCypherDuplicateRelationshipsWithWeightsAggregation(
            DeduplicationStrategy deduplicationStrategy,
            double expectedWeight) {
        String nodes = "MATCH (n) RETURN id(n) AS id";
        String rels = "MATCH (n)-[r]-(m) RETURN id(n) AS source, id(m) AS target, r.weight AS weight";

        Graph graph = runInTransaction(
            db,
            () -> new GraphLoader(db)
                .withLabel(nodes)
                .withRelationshipType(rels)
                .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
                .withDeduplicationStrategy(deduplicationStrategy)
                .load(CypherGraphFactory.class)
        );

        double[] weights = collectTargetProperties(graph, graph.toMappedNodeId(id1));
        assertArrayEquals(new double[]{expectedWeight}, weights);
    }
}
