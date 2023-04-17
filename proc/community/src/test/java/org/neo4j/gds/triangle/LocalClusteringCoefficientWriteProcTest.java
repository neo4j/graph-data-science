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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.test.config.WritePropertyConfigProcTest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.fail;

class LocalClusteringCoefficientWriteProcTest
    extends LocalClusteringCoefficientBaseProcTest<LocalClusteringCoefficientWriteConfig> {

    @TestFactory
    Stream<DynamicTest> configTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig(CypherMapWrapper.empty()))
        ).flatMap(Collection::stream);
    }

    private AlgoBaseProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientWriteConfig, ?> proc() {
        try {
            return getProcedureClazz()
                .getConstructor()
                .newInstance();
        } catch (Exception e) {
            fail("unable to instantiate procedure", e);
        }
        return null;
    }

    @Test
    void testWrite() {
        var query = "CALL gds.localClusteringCoefficient.write('g', { writeProperty: 'localCC' })";

        assertCypherResult(query, List.of(Map.of(
            "averageClusteringCoefficient", closeTo(expectedAverageClusteringCoefficient() / 5, 1e-10),
            "nodeCount", 5L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class),
            "nodePropertiesWritten", 5L,
            "writeMillis", greaterThan(-1L)
        )));

        assertWriteResult(expectedResult, "localCC");
    }

    @Test
    void testWriteSeeded() {
        var query = "CALL gds.localClusteringCoefficient.write('g', { " +
                    "   writeProperty: 'localCC', " +
                    "   triangleCountProperty: 'seed' " +
                    "})";

        assertCypherResult(query, List.of(Map.of(
            "averageClusteringCoefficient", closeTo(expectedAverageClusteringCoefficientSeeded() / 5, 1e-10),
            "nodeCount", 5L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class),
            "nodePropertiesWritten", 5L,
            "writeMillis", greaterThan(-1L)
        )));

        assertWriteResult(expectedResultWithSeeding, "localCC");
    }

    @Override
    public Class<? extends AlgoBaseProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientWriteConfig, ?>> getProcedureClazz() {
        return LocalClusteringCoefficientWriteProc.class;
    }

    @Override
    public LocalClusteringCoefficientWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return LocalClusteringCoefficientWriteConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            mapWrapper = mapWrapper.withString("writeProperty", "writeProperty");
        }
        return mapWrapper;
    }

    private void assertWriteResult(
        Map<String, Double> expectedResult,
        String writeProperty
    ) {
        runQueryWithRowConsumer(String.format(
            "MATCH (n) RETURN n.name AS name, n.%s AS localCC",
            writeProperty
        ), (row) -> {
            double lcc = row.getNumber("localCC").doubleValue();
            var name = row.getString("name");
            var expectedLcc = expectedResult.get(name);
            assertThat(lcc)
                .as("Node with name `%s` has wrong coefficient", name)
                .isEqualTo(expectedLcc);
        });
    }

}
