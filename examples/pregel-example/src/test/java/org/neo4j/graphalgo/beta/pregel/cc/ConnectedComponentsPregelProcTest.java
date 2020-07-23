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
package org.neo4j.graphalgo.beta.pregel.cc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphStreamNodePropertiesProc;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.graphalgo.TestSupport.mapEquals;

class ConnectedComponentsPregelProcTest extends BaseProcTest {

    private static final String TEST_GRAPH =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +
        // {J}
        ", (j:Node { id: 9 })" +
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
            .yields("nodeId", "value");

        HashMap<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query, r -> {
            actual.put(r.getNumber("nodeId").longValue(), r.getNumber("value").doubleValue());
        });

        var expected = Map.of(
            0L, 0D,
            1L, 0D,
            2L, 0D,
            3L, 0D,
            4L, 4D,
            5L, 4D,
            6L, 4D,
            7L, 7D,
            8L, 7D,
            9L, 9D
        );

        assertThat(expected, mapEquals(actual));
    }

    @Test
    void write() {
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "cc")
            .writeMode()
            .addParameter("maxIterations", 10)
            .addParameter("writeProperty", "value")
            .yields();

        runQuery(query);

        HashMap<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) AS nodeId, n.value AS value", r -> {
            actual.put(r.getNumber("nodeId").longValue(), r.getNumber("value").doubleValue());
        });

        var expected = Map.of(
            0L, 0D,
            1L, 0D,
            2L, 0D,
            3L, 0D,
            4L, 4D,
            5L, 4D,
            6L, 4D,
            7L, 7D,
            8L, 7D,
            9L, 9D
        );

        assertThat(expected, mapEquals(actual));
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
            .addParameter("mutateProperty", "value")
            .yields();

        runQuery(mutateQuery);

        var streamQuery = "CALL gds.graph.streamNodeProperty('" + graphName + "', 'value') " +
                          "YIELD nodeId, propertyValue " +
                          "RETURN nodeId, propertyValue AS value";

        HashMap<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(streamQuery, r -> {
            actual.put(r.getNumber("nodeId").longValue(), r.getNumber("value").doubleValue());
        });

        var expected = Map.of(
            0L, 0D,
            1L, 0D,
            2L, 0D,
            3L, 0D,
            4L, 4D,
            5L, 4D,
            6L, 4D,
            7L, 7D,
            8L, 7D,
            9L, 9D
        );

        assertThat(expected, mapEquals(actual));
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
                "computeMillis"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertNotEquals(-1L, row.getNumber("createMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
            }
        );
    }

}
