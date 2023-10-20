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
package org.neo4j.gds.paths.dag.longestPath;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.Path;

import java.util.List;
import java.util.Map;

class DagLongestPathStreamProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n0)-[:T {prop: 8.0}]->(n1)" +
        ", (n0)-[:T {prop: 5.0}]->(n2)" +
        ", (n2)-[:T {prop: 2.0}]->(n1)" +
        ", (n1)-[:T {prop: 0.0}]->(n3)" +
        ", (n2)-[:T {prop: 4.0}]->(n3)";


    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            DagLongestPathStreamProc.class
        );

        var projectQuery = GdsCypher.call("last")
            .graphProject()
            .withRelationshipProperty("prop")
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
    }

    @Test
    void testStreamWithWeights() {
        SoftAssertions assertions = new SoftAssertions();
        long[] a = new long[]{idFunction.of("n0"), idFunction.of("n1"), idFunction.of("n2"), idFunction.of("n3")};

        String query = GdsCypher.call("last")
            .algo("gds.dag.longestPath")
            .streamMode()
            .addParameter("relationshipWeightProperty", "prop")
            .yields();

        var EXPECTED_PATHS = Map.of(
            a[0], new long[]{a[0]},
            a[1], new long[]{a[0], a[1]},
            a[2], new long[]{a[0], a[2]},
            a[3], new long[]{a[0], a[2], a[3]}
        );

        var EXPECTED_COSTS = Map.of(
            a[0], 0d,
            a[1], 8d,
            a[2], 7d,
            a[3], 9d
        );

        var EXPECTED_EDGECOSTS = Map.of(
            a[0], new double[]{0.0},
            a[1], new double[]{0.0, 8.0},
            a[2], new double[]{0.0, 5.0},
            a[3], new double[]{0.0, 5.0, 4.0}
        );

        var rowCount = runQueryWithRowConsumer(
            query,
            row -> {
                assertions.assertThat(row.getNumber("index").longValue()).isBetween(0L, 3L);

                assertions.assertThat(row.getNumber("sourceNode").longValue()).isEqualTo(idFunction.of("n0"));

                long targetNode = row.getNumber("targetNode").longValue();
                assertions.assertThat(targetNode).isIn(EXPECTED_COSTS.keySet());

                assertions.assertThat(row.get("totalCost")).isInstanceOf(Double.class);
                assertions.assertThat(row.getNumber("totalCost").doubleValue()).isCloseTo(
                    EXPECTED_COSTS.get(targetNode),
                    Offset.offset(1e-5)
                );
                assertions.assertThat(row.get("costs")).isInstanceOf(List.class);
                var costsValues = (List<Double>) row.get("costs");
                assertions.assertThat(row.get("nodeIds")).isInstanceOf(List.class);
                var nodeIds = (List<Long>) row.get("nodeIds");

                assertions.assertThat(row.get("path")).isInstanceOf(Path.class);
                long[] expectedPath = EXPECTED_PATHS.get(targetNode);
                double[] expectedCosts = EXPECTED_EDGECOSTS.get(targetNode);

                assertions.assertThat(nodeIds.size()).isEqualTo(expectedPath.length);
                int length = nodeIds.size();
                for (int i = 0; i < length; ++i) {
                    double edgeCost = expectedCosts[i];
                    assertions.assertThat(nodeIds.get(i)).isEqualTo(expectedPath[i]);
                    assertions.assertThat(costsValues.get(i)).isCloseTo(edgeCost, Offset.offset(1e-5));
                }
            }
        );

    }
}
