/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MultiGraphCreateTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class);
    }

    @Test
    void testMultipleGraphLoadsAfterDbChange() {
        String create1 = GdsCypher.call()
            .withNodeLabel("Node1")
            .withRelationshipType("TYPE1", Orientation.UNDIRECTED)
            .graphCreate("graph1")
            .yields();

        String create2 = GdsCypher.call()
            .withNodeLabel("Node2")
            .withRelationshipType("TYPE2", Orientation.UNDIRECTED)
            .graphCreate("graph2")
            .yields();

        runQuery("CREATE (a:Node1), (b:Node1), (a)-[:TYPE1]->(b)");
        runQuery(create1);

        runQuery("CREATE (a:Node2), (b:Node2), (b)-[:TYPE2]->(a)");
        runQuery(create2);

        Graph graph1 = GraphStoreCatalog
            .get("", db.databaseId(), "graph1")
            .graphStore()
            .getGraph(RelationshipType.of("TYPE1"), Optional.empty());
        assertGraph(graph1);

        Graph graph2 = GraphStoreCatalog
            .get("", db.databaseId(), "graph2")
            .graphStore()
            .getGraph(RelationshipType.of("TYPE2"), Optional.empty());
        assertGraph(graph2);
    }

    private void assertGraph(Graph graph1) {
        assertEquals(2, graph1.nodeCount());
        AtomicInteger rels = new AtomicInteger();
        graph1.forEachRelationship(0, (s, t) -> {
            assertEquals(1, t);
            rels.incrementAndGet();
            return true;
        });
        graph1.forEachRelationship(1, (s, t) -> {
            assertEquals(0, t);
            rels.incrementAndGet();
            return true;
        });
        assertEquals(2, rels.get());
    }
}
