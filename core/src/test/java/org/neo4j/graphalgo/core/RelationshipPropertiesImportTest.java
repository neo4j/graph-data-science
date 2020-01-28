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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

class RelationshipPropertiesImportTest {

    private Graph graph;

    private GraphDatabaseAPI db;

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void shutdownGraphDb() {
        db.shutdown();
    }

    @Test
    void testPropertiesOfInterconnectedNodesWithOutgoing() {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Projection.NATURAL);

        checkProperties(0, Projection.NATURAL, 1.0);
        checkProperties(1, Projection.NATURAL, 2.0);
    }

    @Test
    void testPropertiesOfTriangledNodesWithOutgoing() {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Projection.NATURAL);

        checkProperties(0, Projection.NATURAL, 1.0);
        checkProperties(1, Projection.NATURAL, 2.0);
        checkProperties(2, Projection.NATURAL, 3.0);
    }

    @Test
    void testPropertiesOfInterconnectedNodesWithIncoming() {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Projection.REVERSE);

        checkProperties(0, Projection.REVERSE, 2.0);
        checkProperties(1, Projection.REVERSE, 1.0);
    }

    @Test
    void testPropertiesOfTriangledNodesWithIncoming() {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Projection.REVERSE);

        checkProperties(0, Projection.REVERSE, 3.0);
        checkProperties(1, Projection.REVERSE, 1.0);
        checkProperties(2, Projection.REVERSE, 2.0);
    }

    @Test
    void testPropertiesOfInterconnectedNodesWithUndirected() {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Projection.UNDIRECTED);

        checkProperties(0, Projection.UNDIRECTED, 1.0, 2.0);
        checkProperties(1, Projection.UNDIRECTED, 2.0, 1.0);
    }

    @Test
    void testPropertiesOfTriangledNodesWithUndirected() {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Projection.UNDIRECTED);

        checkProperties(0, Projection.UNDIRECTED, 1.0, 3.0);
        checkProperties(1, Projection.UNDIRECTED, 1.0, 2.0);
        checkProperties(2, Projection.UNDIRECTED, 3.0, 2.0);
    }

    private void setup(String cypher, Projection projection) {
        runQuery(db, cypher);

        graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .globalProjection(projection)
            .addRelationshipProperty(PropertyMapping.of("w", 0.0))
            .build()
            .load(HugeGraphFactory.class);

    }

    private void checkProperties(int nodeId, Projection projection, double... expecteds) {
        AtomicInteger i = new AtomicInteger();
        int limit = expecteds.length;
        List<Executable> assertions = new ArrayList<>();

        RelationshipWithPropertyConsumer consumer = (s, t, w) -> {
            String rel = String.format("(%d %s %d)", s, arrow(projection), t);
            if (i.get() >= limit) {
                assertions.add(() -> assertFalse(
                    i.get() >= limit,
                    String.format("Unexpected relationship: %s = %.1f", rel, w)
                ));
                return false;
            }
            final int index = i.getAndIncrement();
            double expectedIterator = expecteds[index];
            assertions.add(() -> assertEquals(
                expectedIterator,
                w,
                1e-4,
                String.format("%s (WRI): %.1f != %.1f", rel, w, expectedIterator)
            ));
            return true;
        };

        graph.forEachRelationship(nodeId, Double.NaN, consumer);
        assertAll(assertions);
    }

    private static String arrow(Projection projection) {
        switch (projection) {
            case NATURAL:
                return "->";
            case REVERSE:
                return "<-";
            case UNDIRECTED:
                return "<->";
            default:
                throw new IllegalArgumentException("Unknown projection: " + projection);
        }
    }
}
