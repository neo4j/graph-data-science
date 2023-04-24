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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class LocalClusteringCoefficientStatsProcTest extends BaseProcTest {

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
            LocalClusteringCoefficientStatsProc.class
        );
        runQuery(
            "CALL gds.graph.project('graph', {A: {label: 'A', properties: 'seed'}}, {T: {orientation: 'UNDIRECTED'}})");
    }

    static Stream<Arguments> queries() {
        return Stream.of(
            arguments(
                "CALL gds.localClusteringCoefficient.stats('graph')",
                (13.0 / 15.0)
            ),
            arguments(
                "CALL gds.localClusteringCoefficient.stats('graph', { triangleCountProperty: 'seed'})",
                (11.0 / 15.0)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("queries")
    void testStats(String query, double expectedCoefficient) {

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("averageClusteringCoefficient"))
                .asInstanceOf(DOUBLE)
                .isCloseTo(expectedCoefficient, Offset.offset(1e-10));

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
    }

}
