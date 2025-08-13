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
import org.assertj.core.api.AssertionsForClassTypes;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdToVariable;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.mem.Estimate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliqueCountingWriteProcTest extends BaseProcTest {

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

    @Inject
    private IdToVariable idToVariable;


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            CliqueCountingWriteProc.class,
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
    void testWriting() {
        @Language("Cypher")
        String query = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds.cliqueCounting")
            .writeMode()
            .addParameter("writeProperty", "perNodeCount")
            .yields();

       var rowCount= runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("computeMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("writeMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("nodePropertiesWritten").longValue()).isEqualTo(5);
            Assertions.assertThat((List<Long>) row.get("globalCount")).containsExactly(4L, 1L);
           assertUserInput(row, "writeProperty", "perNodeCount");

       });

       assertThat(rowCount).isEqualTo(1);

        Map<String, List<Long>> cliqueCountingResult = new HashMap<>(5);
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) AS id, n.perNodeCount AS perNodeCount", row -> {
            long nodeId = row.getNumber("id").longValue();
            var perNodeCount = Arrays.stream((long[]) row.get("perNodeCount")).boxed().toList();
            cliqueCountingResult.put(idToVariable.of(nodeId), perNodeCount);
        });
        Assertions.assertThat(cliqueCountingResult.get("a")).containsExactly(3L, 1L);
        Assertions.assertThat(cliqueCountingResult.get("b")).containsExactly(3L, 1L);
        Assertions.assertThat(cliqueCountingResult.get("c")).containsExactly(3L, 1L);
        Assertions.assertThat(cliqueCountingResult.get("d")).containsExactly(3L, 1L);
        Assertions.assertThat(cliqueCountingResult.get("e")).isEmpty();
    }

    @Disabled
    void testWritingEstimate() {
        //todo?

        @Language("Cypher")
        String query = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds.cliqueCounting")
            .estimationMode(GdsCypher.ExecutionModes.WRITE)
            .addParameter("writeProperty", "perNodeCount")
            .yields("requiredMemory", "treeView", "bytesMin", "bytesMax");

        runQueryWithRowConsumer(query, row -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);

            String bytesHuman = Estimate.humanReadable(row.getNumber("bytesMin").longValue());
            assertNotNull(bytesHuman);
            assertTrue(row.getString("requiredMemory").contains(bytesHuman));
        });
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
            .writeMode()
            .addParameter("writeProperty","foo2")
            .yields();


        var rowCount=runQueryWithRowConsumer(query, row -> {
            AssertionsForClassTypes.assertThat(row.getNumber("preProcessingMillis").longValue()).isNotEqualTo(-1);
            assertThat(row.getNumber("computeMillis").longValue()).isEqualTo(-1);
            assertThat(row.getNumber("nodePropertiesWritten").longValue()).isEqualTo(0);
            Assertions.assertThat((List<Long>) row.get("globalCount")).isEmpty();
        });

        assertThat(rowCount).isEqualTo(1L);
    }

}
