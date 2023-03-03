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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

class ScalePropertiesStreamProcTest extends ScalePropertiesProcTest<ScalePropertiesStreamConfig> {

    @Test
    void stream() {
        loadGraph(GRAPH_NAME);

        var query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.alpha.scaleProperties")
            .streamMode()
            .addParameter("nodeProperties", List.of(NODE_PROP_NAME))
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
        loadGraph(GRAPH_NAME);

        var query = "CALL gds.alpha.scaleProperties.stream('myGraph', {" +
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

    @Override
    public Class<? extends AlgoBaseProc<ScaleProperties, ScaleProperties.Result, ScalePropertiesStreamConfig, ?>> getProcedureClazz() {
        return ScalePropertiesStreamProc.class;
    }

    @Override
    public ScalePropertiesStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return ScalePropertiesStreamConfig.of(mapWrapper);
    }
}
