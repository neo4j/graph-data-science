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
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HeavyCypherGraphFactoryTest {

    private static GraphDatabaseService db;

    private static int id1;
    private static int id2;
    private static int id3;

    @BeforeClass
    public static void setUp() {

        db = TestDatabaseCreator.createTestDatabase();

        db.execute(
                "CREATE (n1 {partition: 6})-[:REL  {prop:1}]->(n2 {foo: 4})-[:REL {prop:2}]->(n3) " +
                   "CREATE (n1)-[:REL {prop:3}]->(n3) " +
                   "RETURN id(n1) AS id1, id(n2) AS id2, id(n3) AS id3").accept(row -> {
            id1 = row.getNumber("id1").intValue();
            id2 = row.getNumber("id2").intValue();
            id3 = row.getNumber("id3").intValue();
            return true;
        });
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }


    @Test
    public void testLoadCypher() {
        String nodes = "MATCH (n) RETURN id(n) as id, n.partition AS partition, n.foo AS foo";
        String rels = "MATCH (n)-[r]->(m) WHERE type(r) = {rel} RETURN id(n) as source, id(m) as target, r.prop as weight ORDER BY id(r) DESC ";

        final HeavyGraph graph = (HeavyGraph) new GraphLoader((GraphDatabaseAPI) db)
                .withParams(MapUtil.map("rel", "REL"))
                .withRelationshipWeightsFromProperty("prop", 0)
                .withLabel(nodes)
                .withRelationshipType(rels)
                .withOptionalNodeProperties(
                        PropertyMapping.of("partition", "partition", 0.0),
                        PropertyMapping.of("foo", "foo", 5.0)
                )
                .withSort(true)
                .load(HeavyCypherGraphFactory.class);

        long node1 = graph.toMappedNodeId(id1);
        long node2 = graph.toMappedNodeId(id2);
        long node3 = graph.toMappedNodeId(id3);

        assertEquals(3, graph.nodeCount());
        assertEquals(2, graph.degree(node1, Direction.OUTGOING));
        assertEquals(1, graph.degree(node2, Direction.OUTGOING));
        assertEquals(0, graph.degree(node3, Direction.OUTGOING));
        AtomicInteger total = new AtomicInteger();
        graph.forEachNode(n -> {
            graph.forEachRelationship(n, Direction.OUTGOING, (s, t, w) -> {
                String rel = "(" + s + ")-->(" + t + ")";
                if (s == id1 && t == id2) {
                    assertEquals("weight of " + rel, 1.0, w, 1e-4);
                } else if (s == id2 && t == id3) {
                    assertEquals("weight of " + rel, 2.0, w, 1e-4);
                } else if (s == id1 && t == id3) {
                    assertEquals("weight of " + rel, 3.0, w, 1e-4);
                } else {
                    fail("Unexpected relationship " + rel);
                }
                total.addAndGet((int) w);
                return true;
            });
            return true;
        });
        assertEquals(6, total.get());

        assertEquals(6.0D, graph.nodeProperties("partition").nodeWeight((long) node1), 0.01);
        assertEquals(5.0D, graph.nodeProperties("foo").nodeWeight((long) node1), 0.01);
        assertEquals(4.0D, graph.nodeProperties("foo").nodeWeight((long) node2), 0.01);
    }
}
