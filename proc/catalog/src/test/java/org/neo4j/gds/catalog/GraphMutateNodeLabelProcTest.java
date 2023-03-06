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
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

@ExtendWith(SoftAssertionsExtension.class)
class GraphMutateNodeLabelProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {longProperty: 1, floatProperty: 15.5})" +
        ", (b:A {longProperty: 2, floatProperty: 23})" +
        ", (c:A {longProperty: 3, floatProperty: 18.3})" +
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
    void mutateNodeLabelMultiLabelProjection(SoftAssertions assertions) {
        // Arrange
        runQuery(
            "CALL gds.graph.project('graph', " +
            "{" +
            "  A: { properties: 'longProperty' }," +
            "  B: { label: 'B' }" +
            "}, " +
            "'*')"
        );

        // Act
        runQuery(
            "CALL gds.alpha.graph.nodeLabel.mutate('graph', 'TestLabel', { nodeFilter: 'n:A AND n.longProperty > 1' }) YIELD nodeCount, nodeLabel, nodeLabelsWritten",
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
                    .as("There should be two nodes having the new label in the in-memory graph")
                    .isEqualTo(2L);

                assertions.assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Assert
        var counter = new LongAdder();
        runQueryWithRowConsumer(
            "CALL gds.graph.nodeProperty.stream('graph', 'longProperty', ['TestLabel'])",
            row -> {
                assertions.assertThat(row.getNumber("nodeId"))
                    .asInstanceOf(LONG)
                    .as("Only nodes `b` and `c` should have the `TestLabel` applied.")
                    .isIn(
                        idFunction.of("b"),
                        idFunction.of("c")
                    );

                assertions.assertThat(row.getNumber("propertyValue"))
                    .asInstanceOf(LONG)
                    .as("The node property should match the applied filter")
                    .isGreaterThan(1);
                counter.increment();
            }
        );
        assertions.assertThat(counter.intValue())
            .as("There should have been two steamed nodes")
            .isEqualTo(2);
    }

    @Test
    void mutateNodeLabelSingleLabelProjection(SoftAssertions assertions) {
        // Arrange
        runQuery(
            "CALL gds.graph.project('graph', " +
            "{" +
            "  A: { properties: 'longProperty' }" +
            "}, " +
            "'*')"
        );

        // Act
        runQuery(
            "CALL gds.alpha.graph.nodeLabel.mutate('graph', 'TestLabel', { nodeFilter: 'n:A AND n.longProperty > 1' }) YIELD nodeCount, nodeLabelsWritten",
            result -> {
                assertions.assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertions.assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be three, including the nodes that didn't get the new label")
                    .isEqualTo(3L);

                assertions.assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be two nodes having the new label in the in-memory graph")
                    .isEqualTo(2L);

                assertions.assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Assert
        var counter = new LongAdder();
        runQueryWithRowConsumer(
            "CALL gds.graph.nodeProperty.stream('graph', 'longProperty', ['TestLabel'])",
            row -> {
                assertions.assertThat(row.getNumber("nodeId"))
                    .asInstanceOf(LONG)
                    .as("Only nodes `b` and `c` should have the `TestLabel` applied.")
                    .isIn(
                        idFunction.of("b"),
                        idFunction.of("c")
                    );

                assertions.assertThat(row.getNumber("propertyValue"))
                    .asInstanceOf(LONG)
                    .as("The node property should match the applied filter")
                    .isGreaterThan(1);

                counter.increment();
            }
        );
        assertions.assertThat(counter.intValue())
            .as("There should have been two steamed nodes")
            .isEqualTo(2);

    }

    @Test
    void mutateNodeLabelStarProjection(SoftAssertions assertions) {
        // Arrange
        runQuery(
            "CALL gds.graph.project('graph', " +
            "'*'," +
            "'*'," +
            "{nodeProperties: {longProperty: {defaultValue: 0}}}" +
            ")"
        );

        // Act
        runQuery(
            "CALL gds.alpha.graph.nodeLabel.mutate('graph', 'TestLabel', { nodeFilter: 'n.longProperty <= 2' }) YIELD nodeCount, nodeLabelsWritten",
            result -> {
                assertions.assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertions.assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
                    .isEqualTo(4L);

                assertions.assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be three nodes having the new label in the in-memory graph")
                    .isEqualTo(3L);

                assertions.assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Assert
        var counter = new LongAdder();
        runQueryWithRowConsumer(
            "CALL gds.graph.nodeProperty.stream('graph', 'longProperty', ['TestLabel'])",
            row -> {
                assertions.assertThat(row.getNumber("nodeId"))
                    .asInstanceOf(LONG)
                    .as("Nodes `a` and `b` and `d` should have the `TestLabel` applied.")
                    .isIn(
                        idFunction.of("a"),
                        idFunction.of("b"),
                        idFunction.of("d")
                    );

                assertions.assertThat(row.getNumber("propertyValue"))
                    .asInstanceOf(LONG)
                    .as("The node property should match the applied filter")
                    .isLessThanOrEqualTo(2);

                counter.increment();
            }
        );
        assertions.assertThat(counter.intValue())
            .as("There should have been three steamed nodes")
            .isEqualTo(3);
    }

    @Test
    void shouldWorkWithFloatProperties(SoftAssertions assertions) {
        // Arrange
        runQuery("CALL gds.graph.project('graph', " +
                 "{" +
                 "  A: { properties: 'floatProperty' }," +
                 "  B: { label: 'B' }" +
                 "}, " +
                 "'*')");


        // Act
        runQuery(
            "CALL gds.alpha.graph.nodeLabel.mutate('graph', 'TestLabel', { nodeFilter: 'n.floatProperty <= 19.0' }) YIELD nodeCount, nodeLabelsWritten",
            result -> {
                assertions.assertThat(result.hasNext()).isTrue();

                var row = result.next();
                assertions.assertThat(row.get("nodeCount"))
                    .as("Total number of nodes in the graph should be four, including the nodes that didn't get the new label")
                    .isEqualTo(4L);

                assertions.assertThat(row.get("nodeLabelsWritten"))
                    .as("There should be two nodes having the new label in the in-memory graph")
                    .isEqualTo(2L);

                assertions.assertThat(result.hasNext()).isFalse();
                return false;
            }
        );

        // Assert
        var counter = new LongAdder();
        runQueryWithRowConsumer(
            "CALL gds.graph.nodeProperty.stream('graph', 'floatProperty', ['TestLabel'])",
            row -> {
                assertions.assertThat(row.getNumber("nodeId"))
                    .asInstanceOf(LONG)
                    .as("Nodes `a` and `c` should have the `TestLabel` applied.")
                    .isIn(
                        idFunction.of("a"),
                        idFunction.of("c")
                    );

                assertions.assertThat(row.getNumber("propertyValue"))
                    .asInstanceOf(DOUBLE)
                    .as("The node property should match the applied filter")
                    .isLessThanOrEqualTo(19d);

                counter.increment();
            }
        );
        assertions.assertThat(counter.intValue())
            .as("There should have been two steamed nodes")
            .isEqualTo(2);
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
        runQuery(
            "CALL gds.graph.project('graph', " +
            "{" +
            "  A: { properties: ['longProperty', 'floatProperty'] }," +
            "  B: { label: 'B' }" +
            "}, " +
            "'*')"
        );

        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(() -> runQuery("CALL gds.alpha.graph.nodeLabel.mutate('graph', 'TestLabel', { nodeFilter: $nodeFilter })",
                Map.of("nodeFilter", nodeFilter)))
            .withMessageContaining("Semantic errors while parsing expression")
            .withMessageContaining("Incompatible types");
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }
}
