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
package org.neo4j.graphalgo.core.heavyweight;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class HeavyCypherGraphFactoryDeduplicationTest {

    private static GraphDatabaseService db;

    private static int id1;
    private static int id2;

    @BeforeClass
    public static void setUp() {

        db = TestDatabaseCreator.createTestDatabase();

        db.execute(
                "MERGE (n1 {id: 1}) " + "" +
                   "MERGE (n2 {id: 2}) " +
                   "CREATE (n1)-[:REL {weight: 4}]->(n2) " +
                   "CREATE (n2)-[:REL {weight: 10}]->(n1) " +
                   "RETURN id(n1) AS id1, id(n2) AS id2").accept(row -> {
            id1 = row.getNumber("id1").intValue();
            id2 = row.getNumber("id2").intValue();
            return true;
        });
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }


    @Test
    public void testLoadCypher() throws Exception {
        String nodes = "MATCH (n) RETURN id(n) as id";
        String rels = "MATCH (n)-[r]-(m) RETURN id(n) as source, id(m) as target, r.weight as weight";

        final HeavyGraph graph = (HeavyGraph) new GraphLoader((GraphDatabaseAPI) db)
                .withLabel(nodes)
                .withRelationshipType(rels)
                .withDuplicateRelationshipsStrategy(DuplicateRelationshipsStrategy.SKIP)
                .load(HeavyCypherGraphFactory.class);

        assertEquals(2, graph.nodeCount());
        assertEquals(1, graph.degree(graph.toMappedNodeId(id1), Direction.OUTGOING));
        assertEquals(1, graph.degree(graph.toMappedNodeId(id2), Direction.OUTGOING));
    }
}
