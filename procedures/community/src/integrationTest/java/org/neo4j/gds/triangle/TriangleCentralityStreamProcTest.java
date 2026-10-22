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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TriangleCentralityStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE " +
            "(a:A)-[:T]->(b:A), " +
            "(b)-[:T]->(c:A), " +
            "(c)-[:T]->(a), " +
            "(a)-[:T]->(d:A)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            TriangleCentralityStreamProc.class
        );

        runQuery("CALL gds.graph.project('graph', 'A', {T: { orientation: 'UNDIRECTED'}})");
    }

    @Test
    void shouldStreamCentralityAndNeighborhoodStatistics() {
        var rowCount = runQueryWithRowConsumer(
            "CALL gds.triangleCentrality.stream('graph')",
            row -> {
                long nodeId = row.getNumber("nodeId").longValue();
                assertThat(row.getNumber("triangleCentrality").doubleValue()).isEqualTo(1.0);

                if (nodeId == idFunction.of("d")) {
                    assertThat(row.getNumber("triangleCount").longValue()).isZero();
                    assertThat(row.getNumber("triangleNeighbors").longValue()).isZero();
                    assertThat(row.getNumber("nonTriangleNeighbors").longValue()).isEqualTo(1);
                    assertThat(row.getNumber("triangleNeighborsTriangleSum").longValue()).isZero();
                    assertThat(row.getNumber("nonTriangleNeighborsTriangleSum").longValue()).isEqualTo(1);
                } else if (nodeId == idFunction.of("a")) {
                    assertThat(row.getNumber("triangleCount").longValue()).isEqualTo(1);
                    assertThat(row.getNumber("triangleNeighbors").longValue()).isEqualTo(2);
                    assertThat(row.getNumber("nonTriangleNeighbors").longValue()).isEqualTo(1);
                    assertThat(row.getNumber("triangleNeighborsTriangleSum").longValue()).isEqualTo(2);
                    assertThat(row.getNumber("nonTriangleNeighborsTriangleSum").longValue()).isZero();
                } else {
                    assertThat(row.getNumber("triangleCount").longValue()).isEqualTo(1);
                    assertThat(row.getNumber("triangleNeighbors").longValue()).isEqualTo(2);
                    assertThat(row.getNumber("nonTriangleNeighbors").longValue()).isZero();
                    assertThat(row.getNumber("triangleNeighborsTriangleSum").longValue()).isEqualTo(2);
                    assertThat(row.getNumber("nonTriangleNeighborsTriangleSum").longValue()).isZero();
                }
            }
        );

        assertThat(rowCount).isEqualTo(4);
    }

    @Test
    void shouldApplyMaxDegreeToCentralityAndNeighborhoods() {
        var rowCount = runQueryWithRowConsumer(
            "CALL gds.triangleCentrality.stream('graph', {maxDegree: 2})",
            row -> {
                long nodeId = row.getNumber("nodeId").longValue();
                double centrality = row.getNumber("triangleCentrality").doubleValue();

                if (nodeId == idFunction.of("a")) {
                    assertThat(centrality).isNaN();
                    assertThat(row.getNumber("triangleCount").longValue()).isEqualTo(-1);
                } else {
                    assertThat(centrality).isZero();
                }

                if (nodeId == idFunction.of("b") || nodeId == idFunction.of("c")) {
                    assertThat(row.getNumber("nonTriangleNeighbors").longValue()).isEqualTo(1);
                } else {
                    assertThat(row.getNumber("nonTriangleNeighbors").longValue()).isZero();
                }
            }
        );

        assertThat(rowCount).isEqualTo(4);
    }

    @Test
    void shouldApplyLabelFilter() {
        var rowCount = runQueryWithRowConsumer(
            "CALL gds.triangleCentrality.stream('graph', {labelFilter: ['A']})",
            row -> {
                long nodeId = row.getNumber("nodeId").longValue();
                if (nodeId == idFunction.of("d")) {
                    assertThat(row.getNumber("triangleCount").longValue()).isZero();
                } else {
                    assertThat(row.getNumber("triangleCount").longValue()).isEqualTo(1);
                }
            }
        );

        assertThat(rowCount).isEqualTo(4);
    }

    @Test
    void shouldRejectTooManyLabelsInLabelFilter() {
        assertThatThrownBy(() -> runQuery(
            "CALL gds.triangleCentrality.stream('graph', {labelFilter: $labels})",
            Map.of("labels", List.of("A", "B", "C", "D"))
        ))
            .hasMessageContaining("up to three elements");
    }

    @Test
    void shouldRejectUnknownLabelInLabelFilter() {
        assertThatThrownBy(() -> runQuery(
            "CALL gds.triangleCentrality.stream('graph', {labelFilter: ['X']})"
        ))
            .hasMessageContaining("TriangleCentrality requires the provided 'labelFilter' node label 'X'");
    }

    @Test
    void shouldRejectDirectedGraphs() {
        runQuery("CALL gds.graph.project('directed', 'A', 'T')");

        assertThatThrownBy(() -> runQuery("CALL gds.triangleCentrality.stream('directed')"))
            .hasMessageContaining("not all undirected");
    }
}
