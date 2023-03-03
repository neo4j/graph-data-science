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
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ScalePropertiesWriteProcTest extends ScalePropertiesProcTest<ScalePropertiesWriteConfig> {

    private static final String WRITE_PROPERTY = "scaledProperty";

    @Override
    public Class<? extends AlgoBaseProc<ScaleProperties, ScaleProperties.Result, ScalePropertiesWriteConfig, ?>> getProcedureClazz() {
        return ScalePropertiesWriteProc.class;
    }

    @Override
    public ScalePropertiesWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return ScalePropertiesWriteConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        var minimalConfig = super.createMinimalConfig(userInput);

        if (!minimalConfig.containsKey("writeProperty")) {
            return minimalConfig.withString("writeProperty", WRITE_PROPERTY);
        }
        return minimalConfig;
    }

    @Test
    void testWrite() {
        loadGraph(GRAPH_NAME);
        String query = GdsCypher
            .call(GRAPH_NAME)
            .algo("gds.beta.scaleProperties")
            .writeMode()
            .addParameter("nodeProperties", List.of(NODE_PROP_NAME))
            .addParameter("scaler", "stdscore")
            .addParameter("writeProperty", WRITE_PROPERTY)
            .yields();

        assertCypherResult(query, List.of(Map.of(
                "nodePropertiesWritten", 6L,
                "scalerStatistics", hasEntry(
                    equalTo(NODE_PROP_NAME),
                    Matchers.allOf(
                        hasEntry(equalTo("std"), hasSize(2)),
                        hasEntry(equalTo("avg"), hasSize(2))
                    )
                ),
                "configuration", isA(Map.class),
                "writeMillis", greaterThan(-1L),
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "postProcessingMillis", 0L
            ))
        );

        runQueryWithRowConsumer(formatWithLocale(
            "MATCH (n) RETURN n.%s AS %s",
            WRITE_PROPERTY,
            WRITE_PROPERTY
        ), row -> {
            var scaledProperties = (double[]) row.get(WRITE_PROPERTY);
            assertThat(scaledProperties).hasSize(2);
        });
    }
}
