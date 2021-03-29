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
package org.neo4j.graphalgo.beta.pregel.cc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphStreamNodePropertiesProc;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.beta.pregel.cc.ConnectedComponentsPregel.COMPONENT;

class ConnectedComponentsPregelProcTest extends BaseProcTest {

    private static final String TEST_GRAPH =
        "CREATE" +
        "  (a:Node { seedProperty: 1 })" +
        ", (b:Node { seedProperty: 2 })" +
        ", (c:Node { seedProperty: 3 })" +
        ", (d:Node { seedProperty: 4 })" +
        ", (e:Node { seedProperty: 2 })" +
        ", (f:Node { seedProperty: 3 })" +
        ", (g:Node { seedProperty: 4 })" +
        ", (h:Node { seedProperty: 3 })" +
        ", (i:Node { seedProperty: 4 })" +
        // {J}
        ", (j:Node { seedProperty: 4 })" +
        // {A, B, C, D}
        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(a)" +
        // {E, F, G}
        ", (e)-[:TYPE]->(f)" +
        ", (f)-[:TYPE]->(g)" +
        ", (g)-[:TYPE]->(e)" +
        // {H, I}
        ", (i)-[:TYPE]->(h)" +
        ", (h)-[:TYPE]->(i)";

    private static final Map<Long, Long> EXPECTED_COMPONENTS = Map.of(
        0L, 0L,
        1L, 0L,
        2L, 0L,
        3L, 0L,
        4L, 4L,
        5L, 4L,
        6L, 4L,
        7L, 7L,
        8L, 7L,
        9L, 9L
    );

    private static final Map<Long, Long> EXPECTED_COMPONENTS_SEEDED = Map.of(
        0L, 1L,
        1L, 1L,
        2L, 1L,
        3L, 1L,
        4L, 2L,
        5L, 2L,
        6L, 2L,
        7L, 3L,
        8L, 3L,
        9L, 4L
    );

    @BeforeEach
    void setup() throws Exception {
        runQuery(TEST_GRAPH);

        registerProcedures(
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class,
            ConnectedComponentsPregelStreamProc.class,
            ConnectedComponentsPregelWriteProc.class,
            ConnectedComponentsPregelMutateProc.class,
            ConnectedComponentsPregelStatsProc.class
        );
    }

    @Test
    void stream() {
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "cc")
            .streamMode()
            .addParameter("maxIterations", 10)
            .yields("nodeId", "values");

        HashMap<Long, Long> actual = new HashMap<>();
        runQueryWithRowConsumer(query, r -> {
            actual.put(
                r.getNumber("nodeId").longValue(),
                ((Map<String, Long>) r.get("values")).get(COMPONENT)
            );
        });

        assertThat(actual).containsExactlyInAnyOrderEntriesOf(EXPECTED_COMPONENTS);
    }

    @Test
    void streamEstimate() {
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "cc")
            .streamEstimation()
            .addParameter("maxIterations", 10)
            .yields("bytesMin", "bytesMax", "nodeCount", "relationshipCount");

        runQueryWithRowConsumer(query, r -> {
            assertEquals(10, r.getNumber("nodeCount").longValue());
            assertEquals(9, r.getNumber("relationshipCount").longValue());
            assertEquals(296_136, r.getNumber("bytesMin").longValue());
            assertEquals(296_136, r.getNumber("bytesMax").longValue());
        });
    }

    @Test
    void streamSeeded() {
        var query = GdsCypher.call()
            .withNodeProperty("seedProperty")
            .loadEverything()
            .algo("example", "pregel", "cc")
            .streamMode()
            .addParameter("maxIterations", 10)
            .addParameter("seedProperty", "seedProperty")
            .yields("nodeId", "values");

        HashMap<Long, Long> actual = new HashMap<>();
        runQueryWithRowConsumer(query, r -> {
            actual.put(
                r.getNumber("nodeId").longValue(),
                ((Map<String, Long>) r.get("values")).get(COMPONENT)
            );
        });

        assertThat(actual).containsExactlyInAnyOrderEntriesOf(EXPECTED_COMPONENTS_SEEDED);
    }

    @Test
    void write() {
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "cc")
            .writeMode()
            .addParameter("maxIterations", 10)
            .addParameter("writeProperty", "value_")
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertNotEquals(-1L, row.getNumber("createMillis"));
            assertNotEquals(-1L, row.getNumber("computeMillis"));
            assertNotEquals(-1L, row.getNumber("writeMillis"));

            assertEquals(1, row.getNumber("ranIterations").longValue());
            assertTrue(row.getBoolean("didConverge"));
        });

        HashMap<Long, Long> actual = new HashMap<>();
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) AS nodeId, n.value_" + COMPONENT + " AS value", r -> {
            actual.put(r.getNumber("nodeId").longValue(), r.getNumber("value").longValue());
        });

        assertThat(actual).containsExactlyInAnyOrderEntriesOf(EXPECTED_COMPONENTS);
    }

    @Test
    void mutate() {
        var graphName = "myGraph";

        var createQuery = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .graphCreate(graphName)
            .yields();

        runQuery(createQuery);

        var mutateQuery = GdsCypher.call()
            .explicitCreation(graphName)
            .algo("example", "pregel", "cc")
            .mutateMode()
            .addParameter("maxIterations", 10)
            .addParameter("mutateProperty", "value_")
            .yields();

        runQueryWithRowConsumer(mutateQuery, row -> {
            assertNotEquals(-1L, row.getNumber("createMillis"));
            assertNotEquals(-1L, row.getNumber("computeMillis"));
            assertNotEquals(-1L, row.getNumber("mutateMillis"));

            assertEquals(1, row.getNumber("ranIterations").longValue());
            assertTrue(row.getBoolean("didConverge"));
        });

        var streamQuery = "CALL gds.graph.streamNodeProperty('" + graphName + "', 'value_" + COMPONENT + "') " +
                          "YIELD nodeId, propertyValue " +
                          "RETURN nodeId, propertyValue AS value";

        HashMap<Long, Long> actual = new HashMap<>();
        runQueryWithRowConsumer(streamQuery, r -> {
            actual.put(r.getNumber("nodeId").longValue(), r.getNumber("value").longValue());
        });

        assertThat(actual).containsExactlyInAnyOrderEntriesOf(EXPECTED_COMPONENTS);
    }

    @Test
    void stats() {
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "cc")
            .statsMode()
            .addParameter("maxIterations", 10)
            .yields(
                "createMillis",
                "computeMillis",
                "ranIterations",
                "didConverge"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertNotEquals(-1L, row.getNumber("createMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));

                assertEquals(1, row.getNumber("ranIterations").longValue());
                assertTrue(row.getBoolean("didConverge"));
            }
        );
    }

}
