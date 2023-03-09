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
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

class ScalePropertiesStatsProcTest extends BaseProcTest {

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
            ScalePropertiesStatsProc.class
        );

        runQuery("CALL gds.graph.project('g', {A: {properties: 'myProp'}}, '*')");
    }

    @Test
    void stats() {
        String query = GdsCypher
            .call("g")
            .algo("gds.beta.scaleProperties")
            .statsMode()
            .addParameter("nodeProperties", List.of("myProp"))
            .addParameter("scaler", "center")
            .yields();

        assertCypherResult(query, List.of(Map.of(
                "scalerStatistics", hasEntry(
                    equalTo("myProp"),
                    Matchers.allOf(
                        hasEntry(equalTo("avg"), hasSize(2))
                    )
                ),
                "configuration", isA(Map.class),
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "postProcessingMillis", 0L
            ))
        );
    }
}
