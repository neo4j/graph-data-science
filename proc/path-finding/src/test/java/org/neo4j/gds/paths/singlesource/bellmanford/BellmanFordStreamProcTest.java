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
package org.neo4j.gds.paths.singlesource.bellmanford;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.Path;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BellmanFordStreamProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0:Node {id:0})," +
        "  (a1:Node {id:1})," +
        "  (a2:Node {id:2})," +
        "  (a3:Node {id:3})," +
        "  (a4:Node {id:4})," +
        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a0)-[:R {weight: -1.0}]->(a2)," +
        "  (a0)-[:R {weight: 10.0}]->(a3), " +
        "  (a3)-[:R {weight: -8.0}]->(a4), " +
        "  (a1)-[:R {weight: 3.0}]->(a4) ";

    @Inject
    public IdFunction idFunction;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            BellmanFordStreamProc.class
        );

        var projectQuery="CALL gds.graph.project('graph', '*' , { R : { properties :'weight' }})";
        runQuery(projectQuery);
    }

    @Test
    void stream() {
        long[] a = new long[]{idFunction.of("a0"), idFunction.of("a1"), idFunction.of("a2"), idFunction.of("a3"), idFunction.of(
            "a4")};
        var EXPECTED_COST = Map.of(
            a[0], 0d,
            a[1], 1d,
            a[2], -1d,
            a[3], 10d,
            a[4], 2d
        );

        var EXPECTED_NODEIDS = Map.of(
            a[0], new long[]{a[0]},
            a[1], new long[]{a[0], a[1]},
            a[2], new long[]{a[0], a[2]},
            a[3], new long[]{a[0], a[3]},
            a[4], new long[]{a[0], a[3], a[4]}
        );

        var EXPECTED_EDGECOSTS = Map.of(
            a[0], new double[]{0.0},
            a[1], new double[]{0.0, 1.0},
            a[2], new double[]{0.0, -1.0},
            a[3], new double[]{0.0, 10.0},
            a[4], new double[]{0.0, 10.0, 2.0}
        );
        var rowCount = runQueryWithRowConsumer("MATCH (n) WHERE  n.id = 0 " +
                                "CALL gds.bellmanFord.stream('graph', {sourceNode: n, relationshipWeightProperty: 'weight'}) " +
                                "YIELD index,sourceNode,targetNode,totalCost,nodeIds,costs,path " +
                                "RETURN index,sourceNode,targetNode,totalCost,nodeIds,costs,path", row -> {

            assertThat(row.getNumber("index").longValue()).isBetween(0L, 4L);

            assertThat(row.getNumber("sourceNode").longValue()).isEqualTo(idFunction.of("a0"));

            long targetNode = row.getNumber("targetNode").longValue();
            assertThat(targetNode).isIn(EXPECTED_COST.keySet());

            assertThat(row.get("totalCost")).isInstanceOf(Double.class);
            assertThat(row.getNumber("totalCost").doubleValue()).isCloseTo(
                EXPECTED_COST.get(targetNode),
                Offset.offset(1e-5)
            );
            assertThat(row.get("costs")).isInstanceOf(List.class);
            var costsValues = (List<Double>) row.get("costs");
            assertThat(row.get("nodeIds")).isInstanceOf(List.class);
            var nodeIds = (List<Long>) row.get("nodeIds");

            assertThat(row.get("path")).isInstanceOf(Path.class);
            long[] expectedPath = EXPECTED_NODEIDS.get(targetNode);
            double[] expectedCosts = EXPECTED_EDGECOSTS.get(targetNode);

            assertThat(nodeIds.size()).isEqualTo(expectedPath.length);
            int length = nodeIds.size();
            for (int i = 0; i < length; ++i) {
                double edgeCost = expectedCosts[i];
                assertThat(nodeIds.get(i)).isEqualTo(expectedPath[i]);
                assertThat(costsValues.get(i)).isCloseTo(edgeCost, Offset.offset(1e-5));
            }
        });
        assertThat(rowCount).isEqualTo(5L);

    }
}
