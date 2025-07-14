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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliqueCountingStreamProcTest extends BaseProcTest {

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
            CliqueCountingStreamProc.class,
            GraphProjectProc.class
        );
        runQuery(
            GdsCypher.call(CLIQUE_COUNTING_GRAPH)
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
    void testStreamingImplicit() {
        @Language("Cypher")
        String yields = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds.cliqueCounting")
            .streamMode()
            .yields("nodeId", "counts");

        Map<String, List<Long>> perNodeCountResult = new HashMap<>(5);
        runQueryWithRowConsumer(yields, (row) -> {
            long nodeId = row.getNumber("nodeId").longValue();
            List<Long> perNodeCount = (List<Long>) row.get("counts");
            perNodeCountResult.put(idToVariable.of(nodeId), perNodeCount);
        });

        Assertions.assertThat(perNodeCountResult.get("a")).containsExactly(3L, 1L);
        Assertions.assertThat(perNodeCountResult.get("b")).containsExactly(3L, 1L);
        Assertions.assertThat(perNodeCountResult.get("c")).containsExactly(3L, 1L);
        Assertions.assertThat(perNodeCountResult.get("d")).containsExactly(3L, 1L);
        Assertions.assertThat(perNodeCountResult.get("e")).hasSize(0);
    }

    @Disabled
    void testStreamingEstimate() {
        @Language("Cypher")
        String query = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds.cliqueCounting")
            .estimationMode(GdsCypher.ExecutionModes.STREAM)
            .yields("requiredMemory", "treeView", "bytesMin", "bytesMax");

        runQueryWithRowConsumer(query, row -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);

            String bytesHuman = Estimate.humanReadable(row.getNumber("bytesMin").longValue());
            assertNotNull(bytesHuman);
            assertTrue(row.getString("requiredMemory").contains(bytesHuman));
        });
    }

//    static Stream<Arguments> communitySizeInputs() {
//        return Stream.of(
//            Arguments.of(Map.of("minCommunitySize", 1), Map.of(
//                "a", 1L,
//                "b", 0L,
//                "c", 0L,
//                "d", 0L
//            )),
//            Arguments.of(Map.of("minCommunitySize", 2), Map.of(
//                "b", 0L,
//                "c", 0L,
//                "d", 0L
//            ))
//        );
//    }

//    @ParameterizedTest
//    @MethodSource("communitySizeInputs")
//    void testStreamingMinCommunitySize(Map<String, Long> parameter, Map<String, Long> expectedResult) {
//        @Language("Cypher")
//        String yields = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds", "k1coloring")
//                .streamMode()
//                .addAllParameters(parameter)
//                .yields("nodeId", "color");
//
//        Map<String, Long> coloringResult = new HashMap<>(4);
//        runQueryWithRowConsumer(yields, (row) -> {
//            long nodeId = row.getNumber("nodeId").longValue();
//            long color = row.getNumber("color").longValue();
//            coloringResult.put(idToVariable.of(nodeId), color);
//        });
//
//        assertEquals(coloringResult, expectedResult);
//    }

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
            .algo("gds",  "cliqueCounting")
            .streamMode()
            .yields();

        var rowCount = runQueryWithRowConsumer(query, resultRow -> {});

        assertThat(rowCount).isEqualTo(0L);

    }

}
