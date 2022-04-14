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
package org.neo4j.gds.kmeans;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class KmeansMutateProcTest  extends BaseProcTest {

    @Neo4jGraph
    static final @Language("Cypher") String DB_CYPHER =
        "CREATE" +
        " (a:Node { weights: [0.0]})" +
        ",(b:Node { weights: [0.1]})" +
        ",(c:Node { weights: [0.2]})" +
        ",(d:Node { weights: [0.3]})" +
        ",(e:Node { weights: [0.4]})";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            KmeansMutateProc.class
        );
    }

    @Test
    void testMutateYields() {
        runQuery(
            GdsCypher.call(DEFAULT_GRAPH_NAME)
                .graphProject()
                .withNodeLabel("Node")
                .withNodeProperty("weights")
                .yields()
        );

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.alpha.kmeans")
            .mutateMode()
            .addParameter("k", 3)
            .addParameter("nodeProperty", "weights")
            .addParameter("mutateProperty", "communityId")
            .yields();


        runQueryWithRowConsumer(query, resultRow -> {
            assertThat(resultRow.get("communityDistribution"))
                .isNotNull()
                .satisfies(distribution -> assertThat(distribution).asInstanceOf(MAP).isNotEmpty());

            assertThat(resultRow.getNumber("preProcessingMillis").longValue())
                .as("preProcessingMillis")
                .isGreaterThanOrEqualTo(0);

            assertThat(resultRow.getNumber("computeMillis").longValue())
                .as("computeMillis")
                .isGreaterThanOrEqualTo(0);

            assertThat(resultRow.getNumber("postProcessingMillis").longValue())
                .as("postProcessingMillis")
                .isGreaterThanOrEqualTo(0);

            assertThat(resultRow.getNumber("mutateMillis").longValue())
                .as("mutateMillis")
                .isGreaterThanOrEqualTo(0);

            assertThat(resultRow.getNumber("nodePropertiesWritten").longValue())
                .as("nodePropertiesWritten")
                .isEqualTo(5);


            assertThat(resultRow.get("configuration"))
                .isNotNull()
                .satisfies(distribution -> assertThat(distribution).asInstanceOf(MAP).isNotEmpty());
        });
    }
}
