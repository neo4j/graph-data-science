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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isA;

class ScalePropertiesStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    @Language("Cypher")
    private static final String DB_CYPHER =
        "CREATE" +
        " (n0:A {myProp: [0, 2]})" +
        ",(n1:A {myProp: [1, 2]})" +
        ",(n2:A {myProp: [2, 2]})" +
        ",(n3:A {myProp: [3, 2]})" +
        ",(n4:A {myProp: [4, 2]})" +
        ",(n5:A {myProp: [5, 2]})";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            ScalePropertiesStreamProc.class
        );

        runQuery("CALL gds.graph.project('g', {A: {properties: 'myProp'}}, '*')");
    }

    @Test
    void stream() {
        var query = GdsCypher
            .call("g")
            .algo("gds.beta.scaleProperties")
            .streamMode()
            .addParameter("nodeProperties", List.of("myProp"))
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
    void estimate() {
        var query = GdsCypher
            .call("g")
            .algo("gds.beta.scaleProperties")
            .streamEstimation()
            .addParameter("nodeProperties", List.of("myProp"))
            .addParameter("scaler", "Mean")
            .yields();

        assertCypherResult(query, List.of(Map.of(
                "mapView", isA(Map.class),
                "treeView", isA(String.class),
                "bytesMax", greaterThanOrEqualTo(0L),
                "heapPercentageMin", greaterThanOrEqualTo(0.0),
                "nodeCount", 6L,
                "relationshipCount", 0L,
                "requiredMemory", isA(String.class),
                "bytesMin", greaterThanOrEqualTo(0L),
                "heapPercentageMax", greaterThanOrEqualTo(0.0)
            ))
        );
    }

    @Test
    void streamLogWithOffset() {
        var query = "CALL gds.beta.scaleProperties.stream('g', {" +
                    "scaler: {type: 'log', offset: 10 }," +
                    "nodeProperties: 'myProp'}) " +
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

    @Test
    void betaDoesNotAllowL1OrL2() {
        var queryL1 = GdsCypher
            .call("g")
            .algo("gds.beta.scaleProperties")
            .streamMode()
            .addParameter("nodeProperties", List.of("myProp"))
            .addParameter("scaler", "L1Norm")
            .yields();
        var queryL2 = GdsCypher
            .call("g")
            .algo("gds.beta.scaleProperties")
            .streamMode()
            .addParameter("nodeProperties", List.of("myProp"))
            .addParameter("scaler", "L2Norm")
            .yields();

        assertError(queryL1, "Unrecognised scaler type specified: `l1norm`");
        assertError(queryL2, "Unrecognised scaler type specified: `l2norm`");
    }

    @Test
    void alphaStreaml1() {
        var query = GdsCypher
            .call("g")
            .algo("gds.alpha.scaleProperties")
            .streamMode()
            .addParameter("nodeProperties", List.of("myProp"))
            .addParameter("scaler", "L1Norm")
            .yields();

        assertCypherResult(query, List.of(
            Map.of("nodeId", 0L, "scaledProperty", List.of(0.0, 1/6D)),
            Map.of("nodeId", 1L, "scaledProperty", List.of(1/15D, 1/6D)),
            Map.of("nodeId", 2L, "scaledProperty", List.of(2/15D, 1/6D)),
            Map.of("nodeId", 3L, "scaledProperty", List.of(0.2, 1/6D)),
            Map.of("nodeId", 4L, "scaledProperty", List.of(4/15D, 1/6D)),
            Map.of("nodeId", 5L, "scaledProperty", List.of(1/3D, 1/6D))
        ));
    }

    @Test
    void alphaStreaml2() {
        var query = GdsCypher
            .call("g")
            .algo("gds.alpha.scaleProperties")
            .streamMode()
            .addParameter("nodeProperties", List.of("myProp"))
            .addParameter("scaler", "L2Norm")
            .yields();

        assertCypherResult(query, List.of(
            Map.of("nodeId", 0L, "scaledProperty", List.of(0.0, 0.4082482904638631)),
            Map.of("nodeId", 1L, "scaledProperty", List.of(0.13483997249264842, 0.4082482904638631)),
            Map.of("nodeId", 2L, "scaledProperty", List.of(0.26967994498529685, 0.4082482904638631)),
            Map.of("nodeId", 3L, "scaledProperty", List.of(0.40451991747794525, 0.4082482904638631)),
            Map.of("nodeId", 4L, "scaledProperty", List.of(0.5393598899705937, 0.4082482904638631)),
            Map.of("nodeId", 5L, "scaledProperty", List.of(0.674199862463242, 0.4082482904638631))
        ));
    }
}
