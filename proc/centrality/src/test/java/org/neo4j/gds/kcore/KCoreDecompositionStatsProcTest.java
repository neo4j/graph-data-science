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
package org.neo4j.gds.kcore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;


class KCoreDecompositionStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (z:node)," +
        "  (a:node)," +
        "  (b:node)," +
        "  (c:node)," +
        "  (d:node)," +
        "  (e:node)," +
        "  (f:node)," +
        "  (g:node)," +
        "  (h:node)," +

        "(a)-[:R]->(b)," +
        "(b)-[:R]->(c)," +
        "(c)-[:R]->(d)," +
        "(d)-[:R]->(e)," +
        "(e)-[:R]->(f)," +
        "(f)-[:R]->(g)," +
        "(g)-[:R]->(h)," +
        "(h)-[:R]->(c)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            KCoreDecompositionStatsProc.class,
            GraphProjectProc.class
        );

        runQuery(
            GdsCypher.call("graph")
                .graphProject()
                .withAnyLabel()
                .withRelationshipType("R", Orientation.UNDIRECTED)
                .yields()
        );
    }

    @Test
    void shouldComputeStats(){

        String query = "CALL gds.kcore.stats('graph')";

        var rowCount = runQueryWithRowConsumer(query, row -> {

            assertThat(row.getNumber("preProcessingMillis"))
                .as("preProcessingMillis")
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .as("computeMillis")
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .as("postProcessingMillis")
                .asInstanceOf(LONG)
                .isEqualTo(-1L);

            
            assertThat(row.getNumber("degeneracy"))
                .as("degeneracy")
                .asInstanceOf(LONG)
                .isEqualTo(2L);

            assertThat(row.get("configuration"))
                .as("configuration")
                .asInstanceOf(MAP)
                .isNotEmpty();
        });

        assertThat(rowCount)
            .as("`mutate` mode should always return one row")
            .isEqualTo(1);


    }

}
