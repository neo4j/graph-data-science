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
package org.neo4j.gds.betweenness;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class BetweennessCentralityStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (d:Node {name: 'd'})" +
        ", (e:Node {name: 'e'})" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(d)" +
        ", (d)-[:REL]->(e)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            BetweennessCentralityStatsProc.class,
            GraphProjectProc.class
        );

        runQuery(
            GdsCypher.call(DEFAULT_GRAPH_NAME)
                .graphProject()
                .loadEverything(Orientation.NATURAL)
                .yields()
        );
    }

    @Test
    void testStats() {
        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("betweenness")
            .statsMode()
            .yields("centralityDistribution", "preProcessingMillis", "computeMillis", "postProcessingMillis");

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("centralityDistribution"))
                .isNotNull()
                .isInstanceOf(Map.class)
                .asInstanceOf(InstanceOfAssertFactories.MAP)
                .containsEntry("min", 0.0)
                .hasEntrySatisfying("max",
                    value -> assertThat(value)
                        .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                        .isEqualTo(4.0, Offset.offset(1e-4))
                )
                .hasEntrySatisfying("mean",
                    value -> assertThat(value)
                        .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                        .isEqualTo(2.0, Offset.offset(1e-4))
                );

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);
        });
    }

    @Test
    void testStatsWithDeprecatedFields() {
        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("betweenness")
            .statsMode()
            .yields("preProcessingMillis", "computeMillis", "postProcessingMillis");

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);
        });
    }
}
