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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class WeightedDegreeCentralityTest {

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

            ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (d)-[:TYPE1 {weight: 5.0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 7.0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 1.0}]->(f)" +

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

    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setupGraphDb() {
        DB = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = DB.beginTx()) {
            DB.execute(DB_CYPHER).close();
            tx.success();
        }
    }

    @AfterAll
    static void shutdownGraphDb() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphTypesTest
    void buildWeightsArray(Class<? extends GraphFactory> graphFactory) {
        final Label label = Label.label("Label1");
        final Map<Long, double[]> expected = new HashMap<>();

        try (Transaction tx = DB.beginTx()) {
            expected.put(DB.findNode(label, "name", "a").getId(), new double[]{});
            expected.put(DB.findNode(label, "name", "b").getId(), new double[]{2.0});
            expected.put(DB.findNode(label, "name", "c").getId(), new double[]{2.0});
            expected.put(DB.findNode(label, "name", "d").getId(), new double[]{5.0, 2.0});
            expected.put(DB.findNode(label, "name", "e").getId(), new double[]{2.0, 7.0, 1.0});
            expected.put(DB.findNode(label, "name", "f").getId(), new double[]{2.0, 2.0});
            expected.put(DB.findNode(label, "name", "g").getId(), new double[]{});
            expected.put(DB.findNode(label, "name", "h").getId(), new double[]{});
            expected.put(DB.findNode(label, "name", "i").getId(), new double[]{});
            expected.put(DB.findNode(label, "name", "j").getId(), new double[]{});
            tx.success();
        }

        final Graph graph;
        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            graph = new GraphLoader(DB)
                    .withLabel("MATCH (n:Label1) RETURN id(n) as id")
                    .withRelationshipType(
                            "MATCH (n:Label1)-[type:TYPE1]->(m:Label1) RETURN id(n) as source,id(m) as target, type.weight AS weight")
                    .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
                    .load(graphFactory);

        } else {
            graph = new GraphLoader(DB)
                    .withLabel(label)
                    .withRelationshipType("TYPE1")
                    .withDirection(Direction.OUTGOING)
                    .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
                    .load(graphFactory);
        }

        WeightedDegreeCentrality degreeCentrality = new WeightedDegreeCentrality(
                graph,
                Pools.DEFAULT,
                1,
                AllocationTracker.EMPTY);
        degreeCentrality.compute(true);

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertArrayEquals(
                    expected.get(nodeId),
                    degreeCentrality.weights().get(i).toArray(),
                    0.01D,
                    "Node#" + nodeId

            );
        });
    }

    @Test
    void shouldThrowIfGraphHasNoRelationshipProperty() {

        Graph graph = new GraphLoader(DB)
                .withLabel(Label.label("Label1"))
                .withRelationshipType("TYPE1")
                .withDirection(Direction.OUTGOING)
                .load(HugeGraphFactory.class);

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> {
            new WeightedDegreeCentrality(
                    graph,
                    Pools.DEFAULT,
                    1,
                    AllocationTracker.EMPTY);
        });

        assertTrue(exception.getMessage().contains("not supported"));
    }
}
