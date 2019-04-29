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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

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
    public void testLoadCypher() throws Exception {
        String nodes = "MATCH (n) RETURN id(n) as id, n.partition AS partition, n.foo AS foo";
        String rels = "MATCH (n)-[r]->(m) WHERE type(r) = {rel} RETURN id(n) as source, id(m) as target, r.prop as weight";

        final HeavyGraph graph = (HeavyGraph) new GraphLoader((GraphDatabaseAPI) db)
                .withParams(MapUtil.map("rel","REL"))
                .withRelationshipWeightsFromProperty("prop", 0)
                .withLabel(nodes)
                .withRelationshipType(rels)
                .withOptionalNodeProperties(
                        PropertyMapping.of("partition", "partition", 0.0),
                        PropertyMapping.of("foo", "foo", 5.0)
                )
                .load(HeavyCypherGraphFactory.class);

        assertEquals(3, graph.nodeCount());
        assertEquals(2, graph.degree(graph.toMappedNodeId(id1), Direction.OUTGOING));
        assertEquals(1, graph.degree(graph.toMappedNodeId(id2), Direction.OUTGOING));
        assertEquals(0, graph.degree(graph.toMappedNodeId(id3), Direction.OUTGOING));
        AtomicInteger total = new AtomicInteger();
        graph.forEachNode(n -> {
            graph.forEachRelationship(n, Direction.OUTGOING, (s, t, r, w) -> {
                total.addAndGet((int) w);
                return true;
            });
            return true;
        });
        assertEquals(6, total.get());

        assertEquals(6.0D, graph.nodeProperties("partition").get(0L), 0.01);
        assertEquals(5.0D, graph.nodeProperties("foo").get(0L), 0.01);
        assertEquals(4.0D, graph.nodeProperties("foo").get(1L), 0.01);
    }
}
