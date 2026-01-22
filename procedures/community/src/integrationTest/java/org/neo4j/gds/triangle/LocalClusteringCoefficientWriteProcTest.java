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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class LocalClusteringCoefficientWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
                                           "(a:A { name: 'a', seed: 2 })-[:T]->(b:A { name: 'b', seed: 2 }), " +
                                           "(b)-[:T]->(c:A { name: 'c', seed: 1 }), " +
                                           "(c)-[:T]->(a), " +
                                           "(a)-[:T]->(d:A { name: 'd', seed: 2 }), " +
                                           "(b)-[:T]->(d), " +
                                           "(c)-[:T]->(d), " +
                                           "(a)-[:T]->(e:A { name: 'e', seed: 2 }), " +
                                           "(b)-[:T]->(e) ";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            LocalClusteringCoefficientWriteProc.class
        );
        runQuery(
            "CALL gds.graph.project('graph', {A: {label: 'A', properties: 'seed'}}, {T: {orientation: 'UNDIRECTED'}})");
    }

    @Test
    void testWrite() {
        var expectedResult = Map.of(
            "a", 2.0 / 3,
            "b", 2.0 / 3,
            "c", 1.0,
            "d", 1.0,
            "e", 1.0
        );

        var query = "CALL gds.localClusteringCoefficient.write('graph', { writeProperty: 'lcc' })";

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("averageClusteringCoefficient"))
                .asInstanceOf(DOUBLE)
                .isCloseTo(13.0 / 15.0, Offset.offset(1e-10));

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

            assertThat(row.get("configuration"))
                .isInstanceOf(Map.class);

        });

        assertThat(rowCount).isEqualTo(1);
        assertWriteResult(expectedResult, "lcc");
    }

    @Test
    void testWriteSeeded() {
        var expectedResult = Map.of(
            "a", 1.0 / 3,
            "b", 1.0 / 3,
            "c", 1.0 / 3,
            "d", 2.0 / 3,
            "e", 2.0
        );

        var query = "CALL gds.localClusteringCoefficient.write('graph', { writeProperty: 'lcc', triangleCountProperty: 'seed' })";

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("averageClusteringCoefficient"))
                .asInstanceOf(DOUBLE)
                .isCloseTo(11.0 / 15.0, Offset.offset(1e-10));

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

            assertThat(row.get("configuration"))
                .isInstanceOf(Map.class);

        });

        assertThat(rowCount).isEqualTo(1);
        assertWriteResult(expectedResult, "lcc");
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
