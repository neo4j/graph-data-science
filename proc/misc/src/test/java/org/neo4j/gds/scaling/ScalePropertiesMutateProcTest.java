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

import org.hamcrest.Matchers;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class ScalePropertiesMutateProcTest extends BaseProcTest {

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

    private static final String EXPECTED_MUTATED_GRAPH =
        " (:A {myProp: [0L, 2L], scaledProperty: [0.0, 1.0]})" +
        ",(:A {myProp: [1L, 2L], scaledProperty: [0.2, 1.0]})" +
        ",(:A {myProp: [2L, 2L], scaledProperty: [0.4, 1.0]})" +
        ",(:A {myProp: [3L, 2L], scaledProperty: [0.6, 1.0]})" +
        ",(:A {myProp: [4L, 2L], scaledProperty: [0.8, 1.0]})" +
        ",(:A {myProp: [5L, 2L], scaledProperty: [1.0, 1.0]})";


    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            ScalePropertiesMutateProc.class
        );

        runQuery("CALL gds.graph.project('g', {A: {properties: 'myProp'}}, '*')");
    }

    @Test
    void mutate() {
        String query = GdsCypher
            .call("g")
            .algo("gds.beta.scaleProperties")
            .mutateMode()
            .addParameter("nodeProperties", List.of("myProp"))
            .addParameter("scaler", "max")
            .addParameter("mutateProperty", "scaledProperty")
            .yields();

        assertCypherResult(query, List.of(Map.of(
                "nodePropertiesWritten", 6L,
                "scalerStatistics", hasEntry(
                    equalTo("myProp"),
                    Matchers.allOf(hasEntry(equalTo("absMax"), hasSize(2)))
                ),
                "configuration", isA(Map.class),
                "mutateMillis", greaterThan(-1L),
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "postProcessingMillis", 0L
            ))
        );

        Graph graph = GraphStoreCatalog.get("", DatabaseId.of(db), "g").graphStore().getUnion();
        assertGraphEquals(fromGdl(EXPECTED_MUTATED_GRAPH), graph);
    }

    @Test
    void estimate() {
        var query = GdsCypher
            .call("g")
            .algo("gds.beta.scaleProperties")
            .mutateEstimation()
            .addParameter("nodeProperties", List.of("myProp"))
            .addParameter("scaler", "max")
            .addParameter("mutateProperty", "scaledProperty")
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
}
