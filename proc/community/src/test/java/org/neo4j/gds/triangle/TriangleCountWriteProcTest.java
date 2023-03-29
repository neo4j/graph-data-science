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
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.test.config.WritePropertyConfigProcTest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class TriangleCountWriteProcTest extends TriangleCountBaseProcTest<TriangleCountWriteConfig> {

    @TestFactory
    Stream<DynamicTest> configTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig())
        ).flatMap(Collection::stream);
    }

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
           "(a:A { name: 'a' })-[:T]->(b:A { name: 'b' }), " +
           "(b)-[:T]->(c:A { name: 'c' }), " +
           "(c)-[:T]->(a), " +
           "(a)-[:T]->(d:A { name: 'd' }), " +
           "(b)-[:T]->(d), " +
           "(c)-[:T]->(d), " +
           "(a)-[:T]->(e:A { name: 'e' }), " +
           "(b)-[:T]->(e) ";

    @Test
    void testWrite() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("triangleCount")
            .writeMode()
            .addParameter("writeProperty", "triangles")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "globalTriangleCount", 5L,
            "nodeCount", 5L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class),
            "nodePropertiesWritten", 5L,
            "writeMillis", greaterThan(-1L)
        )));

        Map<String, Long> expectedResult = Map.of(
            "a", 4L,
            "b", 4L,
            "c", 3L,
            "d", 3L,
            "e", 1L
        );

        assertWriteResult(expectedResult, "triangles");
    }

    @Test
    void testWriteWithMaxDegree() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("triangleCount")
            .writeMode()
            .addParameter("writeProperty", "triangles")
            .addParameter("maxDegree", 2)
            .yields("globalTriangleCount", "nodeCount", "nodePropertiesWritten");

        assertCypherResult(query, List.of(Map.of(
            "globalTriangleCount", 0L,
            "nodeCount", 5L,
            "nodePropertiesWritten", 5L
        )));

        Map<String, Long> expectedResult = Map.of(
            "a", -1L,
            "b", -1L,
            "c", -1L,
            "d", -1L,
            "e", 0L
        );

        assertWriteResult(expectedResult, "triangles");
    }

    @Override
    public Class<? extends AlgoBaseProc<IntersectingTriangleCount, TriangleCountResult, TriangleCountWriteConfig, ?>> getProcedureClazz() {
        return TriangleCountWriteProc.class;
    }

    @Override
    public TriangleCountWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return TriangleCountWriteConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            mapWrapper = mapWrapper.withString("writeProperty", "writeProperty");
        }
        return mapWrapper;
    }

    private void assertWriteResult(
        Map<String, Long> expectedResult,
        String writeProperty
    ) {
        runQueryWithRowConsumer(formatWithLocale(
            "MATCH (n) RETURN n.name AS name, n.%s AS triangles",
            writeProperty
        ), (row) -> {
            String name = row.getString("name");
            Long expectedTriangles = expectedResult.get(name);
            assertThat(row.getNumber("triangles"))
                .asInstanceOf(LONG)
                .isEqualTo(expectedTriangles);
        });
    }

}
