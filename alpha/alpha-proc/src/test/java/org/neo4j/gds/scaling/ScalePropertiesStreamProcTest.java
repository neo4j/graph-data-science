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
package org.neo4j.gds.scaling;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;

import java.util.List;
import java.util.Map;

class ScalePropertiesStreamProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        " (n0:A {id: [0, 2]})" +
        ",(n1:A {id: [1, 2]})" +
        ",(n2:A {id: [2, 2]})" +
        ",(n3:A {id: [3, 2]})" +
        ",(n4:A {id: [4, 2]})" +
        ",(n5:A {id: [5, 2]})";
    private static final String GRAPH_NAME = "graph";

    @BeforeEach
    void setup() throws Exception {
        runQuery(DB_CYPHER);

        registerProcedures(GraphProjectProc.class, ScalePropertiesStreamProc.class);
        var loadQuery = GdsCypher
            .call(GRAPH_NAME)
            .graphProject()
            .withAnyRelationshipType()
            .withNodeLabel("A")
            .withNodeProperty("id")
            .yields();
        runQuery(loadQuery);
    }

    @Test
    void worksOnEmptyGraph() {
        runQuery("CALL gds.beta.graph.project.subgraph('empty', 'graph', 'false', 'false')");

        assertCypherResult(
            "CALL gds.alpha.scaleProperties.stream('empty', {nodeProperties: 'a', scaler: 'mean'})",
            List.of()
        );
    }

    @Test
    void stream() {
        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.alpha.scaleProperties")
            .streamMode()
            .addParameter("nodeProperties", List.of("id"))
            .addParameter("scaler", "Mean")
            .yields();

        assertCypherResult(query, List.of(
            Map.of("nodeId", 0L, "scaledProperty", List.of(-1 / 2D, 0D)),
            Map.of("nodeId", 1L, "scaledProperty", List.of(-3 / 10D, 0D)),
            Map.of("nodeId", 2L, "scaledProperty", List.of(-1 / 10D, 0D)),
            Map.of("nodeId", 3L, "scaledProperty", List.of(1 / 10D, 0D)),
            Map.of("nodeId", 4L, "scaledProperty", List.of(3 / 10D, 0D)),
            Map.of("nodeId", 5L, "scaledProperty", List.of(1 / 2D, 0D))
        ));
    }

    @Test
    void streamLogWithOffset() {
        var query = "CALL gds.alpha.scaleProperties.stream('graph', {" +
                    "scaler: {scaler: 'log', offset: 10 }," +
                    "nodeProperties: 'id'}) " +
                    "yield nodeId, scaledProperty " +
                    "RETURN nodeId, [p in scaledProperty | toInteger(p*100)/100.0] AS scaledProperty";

        assertCypherResult(query, List.of(
            Map.of("nodeId", 0L, "scaledProperty", List.of(2.3, 2.48)),
            Map.of("nodeId", 1L, "scaledProperty", List.of(2.39, 2.48)),
            Map.of("nodeId", 2L, "scaledProperty", List.of(2.48, 2.48)),
            Map.of("nodeId", 3L, "scaledProperty", List.of(2.56, 2.48)),
            Map.of("nodeId", 4L, "scaledProperty", List.of(2.63, 2.48)),
            Map.of("nodeId", 5L, "scaledProperty", List.of(2.7, 2.48))
        ));
    }
}
