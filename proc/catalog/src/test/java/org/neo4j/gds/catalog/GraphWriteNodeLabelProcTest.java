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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

@ExtendWith(SoftAssertionsExtension.class)
class GraphWriteNodeLabelProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {longProperty: 1, floatProperty: 15.5})" +
        ", (b:A {longProperty: 2, floatProperty: 23})" +
        ", (c:A {longProperty: 3, floatProperty: 18.3})" +
        ", (d:B)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphWriteNodeLabelProc.class
        );

        runQuery("CALL gds.graph.project('graph', " +
                 "{" +
                 "  A: { properties: ['longProperty', 'floatProperty'] }," +
                 "  B: { label: 'B' }" +
                 "}, " +
                 "'*')");

    }

    @Test
    void writeFilteredNodeLabelsWithLongPropertyGreaterComparison(SoftAssertions assertions) {
        // First make sure the label we want to write doesn't exist
        runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN count(n) AS nodeCount",
            row -> assertions.assertThat(row.getNumber("nodeCount")).asInstanceOf(LONG).isEqualTo(0L)
        );

        runQuery(
            "CALL gds.alpha.graph.nodeLabel.write('graph', 'TestLabel', { nodeFilter: 'n:A AND n.longProperty > 1' }) YIELD nodeCount, nodeLabel, nodeLabelsWritten",
            result -> {
                assertions.assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertions.assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
                    .isEqualTo(4L);

                assertions.assertThat(row.get("nodeLabel"))
                    .as("The specified node label should be present in the result")
                    .isEqualTo("TestLabel");

                assertions.assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be two nodes having the new label written back to Neo4j")
                    .isEqualTo(2L);


                assertions.assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Check that we actually created the labels in the database
        var rowCount = runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN labels(n) AS updatedLabels, n.longProperty AS longProperty",
            row -> {
                assertions.assertThat(row.get("updatedLabels"))
                    .asList()
                    .containsExactlyInAnyOrder("A", "TestLabel");

                assertions.assertThat(row.getNumber("longProperty"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(1L);
            }
        );
        assertions.assertThat(rowCount).isEqualTo(2);

    }

    @Test
    void writeFilteredNodeLabelsWithLongPropertyLessThanOrEqualComparison(SoftAssertions assertions) {
        // First make sure the label we want to write doesn't exist
        runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN count(n) AS nodeCount",
            row -> assertions.assertThat(row.getNumber("nodeCount")).asInstanceOf(LONG).isEqualTo(0L)
        );

        runQuery(
            "CALL gds.alpha.graph.nodeLabel.write('graph', 'TestLabel', { nodeFilter: 'n:A AND n.longProperty <= 1' }) YIELD nodeCount, nodeLabel, nodeLabelsWritten",
            result -> {
                assertions.assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertions.assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
                    .isEqualTo(4L);

                assertions.assertThat(row.get("nodeLabel"))
                    .as("The specified node label should be present in the result")
                    .isEqualTo("TestLabel");

                assertions.assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be one node having the new label written back to Neo4j")
                    .isEqualTo(1L);


                assertions.assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Check that we actually created the labels in the database
        var rowCount = runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN labels(n) AS updatedLabels, n.longProperty AS longProperty",
            row -> {
                assertions.assertThat(row.get("updatedLabels"))
                    .asList()
                    .containsExactlyInAnyOrder("A", "TestLabel");

                assertions.assertThat(row.getNumber("longProperty"))
                    .asInstanceOf(LONG)
                    .isLessThanOrEqualTo(1L);
            }
        );
        assertions.assertThat(rowCount)
            .as("The MATCH query should return two nodes.")
            .isEqualTo(1);
    }

    @Test
    void writeFilteredNodeLabelsWithFloatPropertyLessThanOrEqualComparison(SoftAssertions assertions) {
        // First make sure the label we want to write doesn't exist
        runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN count(n) AS nodeCount",
            row -> assertions.assertThat(row.getNumber("nodeCount")).asInstanceOf(LONG).isEqualTo(0L)
        );

        runQuery(
            "CALL gds.alpha.graph.nodeLabel.write('graph', 'TestLabel', { nodeFilter: 'n:A AND n.floatProperty <= 22.0' }) YIELD nodeCount, nodeLabel, nodeLabelsWritten",
            result -> {
                assertions.assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertions.assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
                    .isEqualTo(4L);

                assertions.assertThat(row.get("nodeLabel"))
                    .as("The specified node label should be present in the result")
                    .isEqualTo("TestLabel");

                assertions.assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be two nodes having the new label written back to Neo4j")
                    .isEqualTo(2L);


                assertions.assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Check that we actually created the labels in the database
        var rowCount = runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN labels(n) AS updatedLabels, n.floatProperty AS floatProperty",
            row -> {
                assertions.assertThat(row.get("updatedLabels"))
                    .asList()
                    .containsExactlyInAnyOrder("A", "TestLabel");

                assertions.assertThat(row.getNumber("floatProperty"))
                    .asInstanceOf(DOUBLE)
                    .isLessThanOrEqualTo(23d);
            }
        );
        assertions.assertThat(rowCount)
            .as("The MATCH query should return two nodes.")
            .isEqualTo(2);

    }

    @Test
    void writeFilteredNodeLabelsWithFloatPropertyGreaterComparison(SoftAssertions assertions) {
        // First make sure the label we want to write doesn't exist
        runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN count(n) AS nodeCount",
            row -> assertions.assertThat(row.getNumber("nodeCount")).asInstanceOf(LONG).isEqualTo(0L)
        );

        runQuery(
            "CALL gds.alpha.graph.nodeLabel.write('graph', 'TestLabel', { nodeFilter: 'n:A AND n.floatProperty > 19.0' }) YIELD nodeCount, nodeLabel, nodeLabelsWritten",
            result -> {
                assertions.assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertions.assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
                    .isEqualTo(4L);

                assertions.assertThat(row.get("nodeLabel"))
                    .as("The specified node label should be present in the result")
                    .isEqualTo("TestLabel");

                assertions.assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be one node having the new label written back to Neo4j")
                    .isEqualTo(1L);


                assertions.assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Check that we actually created the labels in the database
        var rowCount = runQueryWithRowConsumer(
            "MATCH (n:TestLabel) RETURN labels(n) AS updatedLabels, n.floatProperty AS floatProperty",
            row -> {
                assertions.assertThat(row.get("updatedLabels"))
                    .asList()
                    .containsExactlyInAnyOrder("A", "TestLabel");

                assertions.assertThat(row.getNumber("floatProperty").doubleValue())
                    .isGreaterThan(19d);
            }
        );
        assertions.assertThat(rowCount).isEqualTo(1);

    }

    // Only check two scenarios, the rest are covered in NodeFilterParserTest.
    @ParameterizedTest
    @ValueSource(
        strings = {
            "n.floatProperty > 10",
            "n.longProperty <= 19.6",
        }
    )
    void shouldFailOnIncompatiblePropertyAndValue(String nodeFilter) {
        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(() -> runQuery("CALL gds.alpha.graph.nodeLabel.write('graph', 'TestLabel', { nodeFilter: $nodeFilter })",
                Map.of("nodeFilter", nodeFilter)))
            .withMessageContaining("Semantic errors while parsing expression")
            .withMessageContaining("Incompatible types");
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }
}
