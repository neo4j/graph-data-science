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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class GraphWriteNodeLabelProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {p: 1})" +
        ", (b:A {p: 2})" +
        ", (c:A {p: 3})" +
        ", (d:B)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphWriteNodeLabelProc.class
        );
    }

    @Test
    void writeFilteredNodeLabels() {
        runQuery("CALL gds.graph.project('graph', " +
                 "{" +
                 "  A: { properties: 'p' }," +
                 "  B: { label: 'B' }" +
                 "}, " +
                 "'*')");

        // First make sure the label we want to write doesn't exist
        runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN count(n) AS nodeCount",
            row -> assertThat(row.getNumber("nodeCount")).asInstanceOf(LONG).isEqualTo(0L)
        );

        runQuery(
            "CALL gds.alpha.graph.nodeLabel.writeFiltered('graph', 'TestLabel', { nodeFilter: 'n:A AND n.p > 1.0' }) YIELD nodeLabelsWritten",
            result -> {
                assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertThat(row.get("nodeLabelsWritten"))
                    .isEqualTo(2L);

                assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Check that we actually created the labels in the database
        LongAdder rowCounter = new LongAdder();
        runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN labels(n) AS updatedLabels, n.p AS p",
            row -> {
                assertThat(row.get("updatedLabels"))
                    .asList()
                    .containsExactlyInAnyOrder("A", "TestLabel");

                assertThat(row.getNumber("p"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(1L);

                rowCounter.increment();
            }
        );
        assertThat(rowCounter.intValue()).isEqualTo(2);

    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }
}
