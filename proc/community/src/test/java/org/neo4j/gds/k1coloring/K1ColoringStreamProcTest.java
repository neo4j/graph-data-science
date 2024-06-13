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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class K1ColoringStreamProcTest extends BaseProcTest {

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

    @Inject
    private IdToVariable idToVariable;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            K1ColoringStreamProc.class,
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

    @ParameterizedTest
    @ValueSource(strings = {"gds.k1coloring","gds.beta.k1coloring"})
    void testStreamingImplicit(String tieredProcedure) {
        @Language("Cypher")
        String yields = GdsCypher.call(K1COLORING_GRAPH).algo(tieredProcedure)
            .streamMode()
            .yields("nodeId", "color");

        Map<String, Long> coloringResult = new HashMap<>(4);
        runQueryWithRowConsumer(yields, (row) -> {
            long nodeId = row.getNumber("nodeId").longValue();
            long color = row.getNumber("color").longValue();
            coloringResult.put(idToVariable.of(nodeId), color);
        });

        assertNotEquals(coloringResult.get("a"), coloringResult.get("b"));
        assertNotEquals(coloringResult.get("a"), coloringResult.get("c"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.k1coloring","gds.beta.k1coloring"})
    void testStreamingEstimate(String tieredProcedure) {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo(tieredProcedure)
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

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
            Arguments.of(Map.of("minCommunitySize", 1), Map.of(
                "a", 1L,
                "b", 0L,
                "c", 0L,
                "d", 0L
            )),
            Arguments.of(Map.of("minCommunitySize", 2), Map.of(
                "b", 0L,
                "c", 0L,
                "d", 0L
            ))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testStreamingMinCommunitySize(Map<String, Long> parameter, Map<String, Long> expectedResult) {
        @Language("Cypher")
        String yields = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "k1coloring")
                .streamMode()
                .addAllParameters(parameter)
                .yields("nodeId", "color");

        Map<String, Long> coloringResult = new HashMap<>(4);
        runQueryWithRowConsumer(yields, (row) -> {
            long nodeId = row.getNumber("nodeId").longValue();
            long color = row.getNumber("color").longValue();
            coloringResult.put(idToVariable.of(nodeId), color);
        });

        assertEquals(coloringResult, expectedResult);
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
            .algo("gds",  "k1coloring")
            .streamMode()
            .yields();

        var rowCount = runQueryWithRowConsumer(query, resultRow -> {});

        assertThat(rowCount).isEqualTo(0L);

    }

}
