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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class TriangleCountWriteProcTest extends BaseProcTest {

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

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            TriangleCountWriteProc.class
        );

        runQuery("CALL gds.graph.project('graph', 'A', {T: { orientation: 'UNDIRECTED'}})");
    }

    @Test
    void shouldWrite() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("triangleCount")
            .writeMode()
            .addParameter("writeProperty", "triangles")
            .yields();

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("globalTriangleCount"))
                .asInstanceOf(LONG)
                .isEqualTo(5L);

            assertThat(row.getNumber("nodeCount"))
                .asInstanceOf(LONG)
                .isEqualTo(5L);

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("writeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.get("configuration"))
                .isInstanceOf(Map.class);

            assertThat(row.getNumber("nodePropertiesWritten"))
                .asInstanceOf(LONG)
                .isEqualTo(5L);
        });
        assertThat(rowCount).isEqualTo(1L);

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
    void shouldWriteWithMaxDegree() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("triangleCount")
            .writeMode()
            .addParameter("writeProperty", "triangles")
            .addParameter("maxDegree", 2)
            .yields("globalTriangleCount", "nodeCount", "nodePropertiesWritten");

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("globalTriangleCount"))
                .asInstanceOf(LONG)
                .isEqualTo(0L);

            assertThat(row.getNumber("nodeCount"))
                .asInstanceOf(LONG)
                .isEqualTo(5L);

            assertThat(row.getNumber("nodePropertiesWritten"))
                .asInstanceOf(LONG)
                .isEqualTo(5L);
        });
        assertThat(rowCount).isEqualTo(1L);

        Map<String, Long> expectedResult = Map.of(
            "a", -1L,
            "b", -1L,
            "c", -1L,
            "d", -1L,
            "e", 0L
        );

        assertWriteResult(expectedResult, "triangles");
    }



    private void assertWriteResult(
        Map<String, Long> expectedResult,
        String writeProperty
    ) {
        var rowCount = runQueryWithRowConsumer(formatWithLocale(
            "MATCH (n) RETURN n.name AS name, n.%s AS triangles",
            writeProperty
        ), (row) -> {
            String name = row.getString("name");
            Long expectedTriangles = expectedResult.get(name);
            assertThat(row.getNumber("triangles"))
                .asInstanceOf(LONG)
                .isEqualTo(expectedTriangles);
        });
        assertThat(rowCount).isEqualTo(expectedResult.keySet().size());
    }

}
