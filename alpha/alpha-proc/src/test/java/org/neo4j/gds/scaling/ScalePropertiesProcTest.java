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
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class ScalePropertiesProcTest<CONFIG extends ScalePropertiesBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<ScaleProperties, CONFIG, ScaleProperties.Result> {

    static final String GRAPH_NAME = "myGraph";
    static final String NODE_PROP_NAME = "myProp";

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

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

    @Override
    public List<String> nodeProperties() {
        return List.of(NODE_PROP_NAME);
    }

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
    }

    @Override
    public void assertResultEquals(ScaleProperties.Result result1, ScaleProperties.Result result2) {
        var scaledProps1 = result1.scaledProperties();
        var scaledProps2 = result2.scaledProperties();
        assertEquals(scaledProps1.size(), scaledProps2.size(), "Scaled properties sizes are supposed to be equal.");
        long nodeCount = scaledProps1.size();
        for (long i = 0; i < nodeCount; i++) {
            assertThat(scaledProps1.get(i)).containsExactly(scaledProps2.get(i));
        }
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        return userInput
            .withString("scaler", "mean")
            .withEntry("nodeProperties", List.of(NODE_PROP_NAME));
    }
}
