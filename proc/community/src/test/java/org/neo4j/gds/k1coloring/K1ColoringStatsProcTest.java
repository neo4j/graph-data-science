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
package org.neo4j.gds.k1coloring;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class K1ColoringStatsProcTest extends BaseProcTest {

    private static final String TEST_USERNAME = Username.EMPTY_USERNAME.username();
    private static final String K1COLORING_GRAPH = "myGraph";

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        " (a)" +
        ",(b)" +
        ",(c)" +
        ",(d)" +
        ",(a)-[:REL]->(b)" +
        ",(a)-[:REL]->(c)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            K1ColoringStatsProc.class,
            GraphProjectProc.class
        );
        runQuery(
            GdsCypher.call(K1COLORING_GRAPH)
                .graphProject()
                .loadEverything(Orientation.NATURAL)
                .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStats() {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH)
            .algo("gds", "beta", "k1coloring")
            .statsMode()
            .yields();

        var rowCount=runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("computeMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("nodeCount").longValue()).isEqualTo(4);
            assertThat(row.getNumber("colorCount").longValue()).isEqualTo(2);
            assertThat(row.getNumber("ranIterations").longValue()).isLessThan(3);
            assertThat(row.getBoolean("didConverge")).isTrue();

        });

        assertThat(rowCount).isEqualTo(1L);

    }

    @Test
    void testRunOnEmptyGraph() {
        // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later

        runQuery("CALL db.createLabel('X')");
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        String  projectQuery = GdsCypher.call("foo")
                .graphProject().withNodeLabel("X").yields();
        runQuery(projectQuery);

        String query = GdsCypher.call("foo")
            .algo("gds", "beta", "k1coloring")
            .statsMode()
            .yields();


        var rowCount=runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isNotEqualTo(-1);
            assertThat(row.getNumber("computeMillis").longValue()).isEqualTo(0);
            assertThat(row.getNumber("nodeCount").longValue()).isEqualTo(0);
            assertThat(row.getNumber("colorCount").longValue()).isEqualTo(0);
            assertThat(row.getNumber("ranIterations").longValue()).isEqualTo(0);
            assertThat(row.getBoolean("didConverge")).isFalse();
        });

        assertThat(rowCount).isEqualTo(1L);

    }

}
