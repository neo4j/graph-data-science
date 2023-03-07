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
package org.neo4j.gds.degree;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.test.config.WritePropertyConfigProcTest;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class DegreeCentralityWriteProcTest extends DegreeCentralityProcTest<DegreeCentralityWriteConfig> {

    @Override
    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig())
        ).flatMap(Collection::stream);
    }

    @Override
    public Class<? extends AlgoBaseProc<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityWriteConfig, ?>> getProcedureClazz() {
        return DegreeCentralityWriteProc.class;
    }

    @Override
    public DegreeCentralityWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return DegreeCentralityWriteConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            mapWrapper = mapWrapper.withString("writeProperty", DEFAULT_RESULT_PROPERTY);
        }
        return mapWrapper;
    }

    @Test
    void testWrite() {
        String writeQuery = GdsCypher.call(GRAPH_NAME)
            .algo("degree")
            .writeMode()
            .addParameter("writeProperty", DEFAULT_RESULT_PROPERTY)
            .yields();

        runQueryWithRowConsumer(writeQuery, row -> {
            Assertions.assertThat(row.get("centralityDistribution"))
                .isNotNull()
                .isInstanceOf(Map.class)
                .asInstanceOf(InstanceOfAssertFactories.MAP)
                .containsEntry("min", 0.0)
                .hasEntrySatisfying("max",
                    value -> Assertions.assertThat(value)
                        .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                        .isEqualTo(3.0, Offset.offset(1e-4))
                );

            Assertions.assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            Assertions.assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            Assertions.assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            Assertions.assertThat(row.getNumber("writeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            Assertions.assertThat(row.getNumber("nodePropertiesWritten"))
                .asInstanceOf(LONG)
                .isEqualTo(10L);
        });
    }
}
