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
package gds.example;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.GdlGraph;

import java.util.HashMap;
import java.util.Map;

import static gds.example.ExamplePregelComputation.KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class ExamplePregelComputationProcTest extends BaseProcTest {

    @GdlGraph
    private static final String MY_TEST_GRAPH =
        "CREATE" +
        "  (alice)" +
        ", (bob)" +
        ", (eve)" +
        ", (alice)-[:LIKES]->(bob)" +
        ", (bob)-[:LIKES]->(alice)" +
        ", (eve)-[:DISLIKES]->(alice)" +
        ", (eve)-[:DISLIKES]->(bob)";

    @BeforeEach
    void setup() throws Exception {
        runQuery(MY_TEST_GRAPH);
        registerProcedures(ExamplePregelComputationStreamProc.class, GraphProjectProc.class);
    }

    @Test
    void stream() {
        var createQuery = GdsCypher.call("graph")
            .graphProject()
            .yields();
        runQuery(createQuery);

        // GdsCypher creates procedure statements, such as:
        // CALL pregel.example.stream('graph', {
        //  maxIterations: 10
        // }) YIELD nodeId, values

        var query = GdsCypher.call("graph")
            .algo("pregel", "example")
            .streamMode()
            .addParameter("maxIterations", 10)
            .yields("nodeId", "values");

        var actual = new HashMap<Long, Long>();

        runQueryWithRowConsumer(query, r -> {
            actual.put(
                r.getNumber("nodeId").longValue(),
                ((Map<String, Long>) r.get("values")).get(KEY)
            );
        });

        Map<Long, Long> expected = Map.of(
            0L, 0L,
            1L, 1L,
            2L, 2L
        );

        assertThat(expected).containsExactlyInAnyOrderEntriesOf(actual);
    }

    @Test
    void memoryEstimation() {
        runQuery("CALL gds.graph.project('graph', '*', '*')");

        var query = "CALL pregel.example.stream.estimate('graph', {maxIterations: 10}) YIELD bytesMin, bytesMax";

        var rowCount = runQueryWithRowConsumer(query, r -> {
            assertThat(r.getNumber("bytesMin")).asInstanceOf(LONG).isPositive();
            assertThat(r.getNumber("bytesMax")).asInstanceOf(LONG).isPositive();
        });

        assertThat(rowCount)
            .as("Memory estimation should always return a single row.")
            .isEqualTo(1);
    }
}
