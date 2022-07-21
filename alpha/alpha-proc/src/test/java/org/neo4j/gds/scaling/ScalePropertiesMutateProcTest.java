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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class ScalePropertiesMutateProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        " (n0:A {id: 0})" +
        ",(n1:A {id: 1})" +
        ",(n2:A {id: 2})" +
        ",(n3:A {id: 3})" +
        ",(n4:A {id: 4})" +
        ",(n5:A {id: 5})";
    private static final String GRAPH_NAME = "graph";

    @BeforeEach
    void setup() throws Exception {
        runQuery(DB_CYPHER);

        registerProcedures(GraphProjectProc.class, ScalePropertiesMutateProc.class);
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
    void mutate() {
        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.alpha.scaleProperties")
            .mutateMode()
            .addParameter("nodeProperties", List.of("id"))
            .addParameter("scaler", "Mean")
            .addParameter("mutateProperty", "mean")
            .yields();

        assertCypherResult(query, List.of(
            Map.of(
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "mutateMillis", greaterThan(-1L),
                "postProcessingMillis", 0L,
                "configuration", isA(Map.class),
                "nodePropertiesWritten", 6L
            )
        ));

        Graph actualGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), GRAPH_NAME).graphStore().getUnion();

        assertGraphEquals(
            fromGdl(
                " (n0:A {id: 0, mean: [-0.5]})" +
                ",(n1:A {id: 1, mean: [-0.3]})" +
                ",(n2:A {id: 2, mean: [-0.1]})" +
                ",(n3:A {id: 3, mean: [0.1]})" +
                ",(n4:A {id: 4, mean: [0.3]})" +
                ",(n5:A {id: 5, mean: [0.5]})"
            ), actualGraph);
    }

}
