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
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

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
        var rowCount = runQueryWithRowConsumer(
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
            }
        );
        assertions.assertThat(rowCount)
            .as("There should have been two steamed nodes")
            .isEqualTo(2);

    }
    
    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }
}
