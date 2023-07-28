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
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class ExamplePregelComputationWriteProcTest extends BaseProcTest {

    @Neo4jGraph
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
        registerProcedures(ExamplePregelComputationWriteProc.class, GraphProjectProc.class);
    }

    @Test
    void write() {
        runQueryWithRowConsumer("MATCH (n) WHERE n.pregelkey IS NOT NULL RETURN count(n) AS count ", row -> {
            assertThat(row.getNumber("count"))
                .asInstanceOf(LONG)
                .as("There should be no nodes having the `pregelkey` property")
                .isEqualTo(0);
        });

        runQuery("CALL gds.graph.project('graph', '*', '*')");
        runQuery("CALL pregel.example.write('graph', { maxIterations: 10, writeProperty: 'pregel' })");

        runQueryWithRowConsumer("MATCH (n) WHERE n.pregelkey IS NOT NULL RETURN count(n) AS count ", row -> {
            assertThat(row.getNumber("count"))
                .asInstanceOf(LONG)
                .as("All nodes should have the `pregelkey` property")
                .isEqualTo(3);
        });
    }
}
