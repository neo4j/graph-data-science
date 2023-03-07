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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

class ScalePropertiesMutateProcTest extends ScalePropertiesProcTest<ScalePropertiesMutateConfig>
    implements MutateNodePropertyTest<ScaleProperties, ScalePropertiesMutateConfig, ScaleProperties.Result> {

    private static final String MUTATE_PROPERTY = "scaledProperty";

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        var minimalConfig = super.createMinimalConfig(userInput);

        if (!minimalConfig.containsKey("mutateProperty")) {
            return minimalConfig.withString("mutateProperty", MUTATE_PROPERTY);
        }
        return minimalConfig;
    }

    @Override
    public Class<? extends AlgoBaseProc<ScaleProperties, ScaleProperties.Result, ScalePropertiesMutateConfig, ?>> getProcedureClazz() {
        return ScalePropertiesMutateProc.class;
    }

    @Override
    public ScalePropertiesMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return ScalePropertiesMutateConfig.of(mapWrapper);
    }

    @Override
    public String mutateProperty() {
        return MUTATE_PROPERTY;
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.DOUBLE_ARRAY;
    }

    @Override
    public String expectedMutatedGraph() {
        return "CREATE" +
               " (n0 {myProp: [0L, 2L], scaledProperty: [-0.5, 0.0]})" +
               ",(n1 {myProp: [1L, 2L], scaledProperty: [-0.3, 0.0]})" +
               ",(n2 {myProp: [2L, 2L], scaledProperty: [-0.1, 0.0]})" +
               ",(n3 {myProp: [3L, 2L], scaledProperty: [0.1, 0.0]})" +
               ",(n4 {myProp: [4L, 2L], scaledProperty: [0.3, 0.0]})" +
               ",(n5 {myProp: [5L, 2L], scaledProperty: [0.5, 0.0]})";
    }

    @Test
    void testGetsStatistics() {
        loadGraph(GRAPH_NAME);
        String query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.alpha.scaleProperties")
            .mutateMode()
            .addParameter("nodeProperties", List.of(NODE_PROP_NAME))
            .addParameter("scaler", "max")
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        assertCypherResult(query, List.of(Map.of(
                "nodePropertiesWritten", 6L,
                "scalerStatistics", hasEntry(
                    equalTo(NODE_PROP_NAME),
                    Matchers.allOf(hasEntry(equalTo("absMax"), hasSize(2)))
                ),
                "configuration", isA(Map.class),
                "mutateMillis", greaterThan(-1L),
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "postProcessingMillis", 0L
            ))
        );
    }
}
