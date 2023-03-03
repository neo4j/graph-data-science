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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class GraphMutateNodeLabelProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A { p: 1.0 })" +
        ", (b:A { p: 2.0 })" +
        ", (c:A { p: 3.0 })" +
        ", (d:B)";

    @Inject
    IdFunction idFunction;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphStreamNodePropertiesProc.class,
            GraphListProc.class,
            GraphMutateNodeLabelProc.class
        );
    }

    @Test
    void mutateNodeLabelMultiLabelProjection() {
        // Arrange
        runQuery(
            "CALL gds.graph.project('graph', " +
            "{" +
            "  A: { properties: 'p' }," +
            "  B: { label: 'B' }" +
            "}, " +
            "'*')"
        );

        // Act
        runQuery(
            "CALL gds.alpha.graph.nodeLabel.mutate('graph', 'TestLabel', { nodeFilter: 'n:A AND n.p > 1.0' }) YIELD nodeCount, nodeLabel, nodeLabelsWritten",
            result -> {
                assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
                    .isEqualTo(4L);

                assertThat(row.get("nodeLabel"))
                    .as("The specified node label should be present in the result")
                    .isEqualTo("TestLabel");

                assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be two nodes having the new label in the in-memory graph")
                    .isEqualTo(2L);

                assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Assert
        var rowCount = runQueryWithRowConsumer(
            "CALL gds.graph.nodeProperty.stream('graph', 'p', ['TestLabel'])",
            row -> {
                assertThat(row.getNumber("nodeId"))
                    .asInstanceOf(LONG)
                    .as("Only nodes `b` and `c` should have the `TestLabel` applied.")
                    .isIn(
                        idFunction.of("b"),
                        idFunction.of("c")
                    );

                assertThat(row.getNumber("propertyValue"))
                    .asInstanceOf(DOUBLE)
                    .as("The node property should match the applied filter")
                    .isGreaterThan(1d);
            }
        );
        assertThat(rowCount)
            .as("There should have been two steamed nodes")
            .isEqualTo(2);
    }

    @Test
    void mutateNodeLabelSingleLabelProjection() {
        // Arrange
        runQuery(
            "CALL gds.graph.project('graph', " +
            "{" +
            "  A: { properties: 'p' }" +
            "}, " +
            "'*')"
        );

        // Act
        runQuery(
            "CALL gds.alpha.graph.nodeLabel.mutate('graph', 'TestLabel', { nodeFilter: 'n:A AND n.p > 1.0' }) YIELD nodeCount, nodeLabelsWritten",
            result -> {
                assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be three, including the nodes that didn't get the new label")
                    .isEqualTo(3L);

                assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be two nodes having the new label in the in-memory graph")
                    .isEqualTo(2L);

                assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Assert
        var rowCount = runQueryWithRowConsumer(
            "CALL gds.graph.nodeProperty.stream('graph', 'p', ['TestLabel'])",
            row -> {
                assertThat(row.getNumber("nodeId"))
                    .asInstanceOf(LONG)
                    .as("Only nodes `b` and `c` should have the `TestLabel` applied.")
                    .isIn(
                        idFunction.of("b"),
                        idFunction.of("c")
                    );

                assertThat(row.getNumber("propertyValue"))
                    .asInstanceOf(DOUBLE)
                    .as("The node property should match the applied filter")
                    .isGreaterThan(1d);
            }
        );
        assertThat(rowCount)
            .as("There should have been two steamed nodes")
            .isEqualTo(2);

    }

    @Test
    void mutateNodeLabelStarProjection() {
        // Arrange
        runQuery(
            "CALL gds.graph.project('graph', " +
            "'*'," +
            "'*'," +
            "{ nodeProperties: {p: {defaultValue: 0.0}}}" +
            ")"
        );

        // Act
        runQuery(
            "CALL gds.alpha.graph.nodeLabel.mutate('graph', 'TestLabel', { nodeFilter: 'n.p > 2.0' }) YIELD nodeCount, nodeLabelsWritten",
            result -> {
                assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
                    .isEqualTo(4L);

                assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be two nodes having the new label in the in-memory graph")
                    .isEqualTo(1L);

                assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Assert
        var rowCount = runQueryWithRowConsumer(
            "CALL gds.graph.nodeProperty.stream('graph', 'p', ['TestLabel'])",
            row -> {
                assertThat(row.getNumber("nodeId"))
                    .asInstanceOf(LONG)
                    .as("Only node `c` should have the `TestLabel` applied.")
                    .isEqualTo(
                        idFunction.of("c")
                    );

                assertThat(row.getNumber("propertyValue"))
                    .asInstanceOf(DOUBLE)
                    .as("The node property should match the applied filter")
                    .isGreaterThan(2d);
            }
        );

        assertThat(rowCount)
            .as("There should have been one steamed node")
            .isEqualTo(1);

    }
}
