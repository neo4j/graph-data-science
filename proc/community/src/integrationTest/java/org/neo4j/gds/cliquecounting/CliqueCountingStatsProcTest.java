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
package org.neo4j.gds.cliquecounting;

import org.assertj.core.api.Assertions;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class CliqueCountingStatsProcTest extends BaseProcTest {

    //todo

    private static final String CLIQUE_COUNTING_GRAPH = "myGraph";

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
            " (a)" +
            ",(b)" +
            ",(c)" +
            ",(d)" +
            ",(e)" +
            ",(a)-[:REL]->(b)" +
            ",(a)-[:REL]->(c)" +
            ",(a)-[:REL]->(d)" +
            ",(b)-[:REL]->(c)" +
            ",(b)-[:REL]->(d)" +
            ",(c)-[:REL]->(d)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            CliqueCountingStatsProc.class,
            GraphProjectProc.class
        );
        runQuery(
            GdsCypher.call(CLIQUE_COUNTING_GRAPH)
                .graphProject()
                .loadEverything(Orientation.UNDIRECTED)
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
        String query = GdsCypher.call(CLIQUE_COUNTING_GRAPH)
            .algo("gds.cliqueCounting")
            .statsMode()
            .yields();

        var rowCount=runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("computeMillis").longValue()).isGreaterThanOrEqualTo(0);
            Assertions.assertThat((List<Long>) row.get("globalCount")).containsExactly(4L,1L);
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
                .graphProject().loadEverything(Orientation.UNDIRECTED).yields();
        runQuery(projectQuery);

        String query = GdsCypher.call("foo")
            .algo("gds", "cliqueCounting")
            .statsMode()
            .yields();


        var rowCount=runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isNotEqualTo(-1);
            assertThat(row.getNumber("computeMillis").longValue()).isEqualTo(-1);
            Assertions.assertThat(((List<Long>) row.get("globalCount")).size()).isEqualTo(0);
        });

        assertThat(rowCount).isEqualTo(1L);

    }

}
