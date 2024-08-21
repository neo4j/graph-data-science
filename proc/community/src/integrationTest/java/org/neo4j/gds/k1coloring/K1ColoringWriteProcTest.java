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

import org.assertj.core.api.AssertionsForClassTypes;
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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class K1ColoringWriteProcTest extends BaseProcTest {

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
            K1ColoringWriteProc.class,
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
    void testWriting(String tieredProcedure) {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo(tieredProcedure)
            .writeMode()
            .addParameter("writeProperty", "color")
            .yields();

       var rowCount= runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("computeMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("writeMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("nodeCount").longValue()).isEqualTo(4);
            assertThat(row.getNumber("colorCount").longValue()).isEqualTo(2);
            assertThat(row.getNumber("ranIterations").longValue()).isLessThan(3);
            assertThat(row.getBoolean("didConverge")).isTrue();
           assertUserInput(row, "writeProperty", "color");

       });

       assertThat(rowCount).isEqualTo(1);

        Map<String, Long> coloringResult = new HashMap<>(4);
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) AS id, n.color AS color", row -> {
            long nodeId = row.getNumber("id").longValue();
            long color = row.getNumber("color").longValue();
            coloringResult.put(idToVariable.of(nodeId), color);
        });
        assertThat(coloringResult.get("a")).isNotEqualTo(coloringResult.get("b"));
        assertThat(coloringResult.get("a")).isNotEqualTo(coloringResult.get("c"));

    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.k1coloring","gds.beta.k1coloring"})
    void testWritingEstimate(String tieredProcedure) {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo(tieredProcedure)
            .estimationMode(GdsCypher.ExecutionModes.WRITE)
            .addParameter("writeProperty", "color")
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
    void testWriteWithMinCommunitySize(Map<String, Long> parameter, Map<String, Long> expectedResult) {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "k1coloring")
                .writeMode()
                .addParameter("writeProperty", "color")
                .addAllParameters(parameter)
                .yields("nodeCount", "colorCount");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(4L, row.getNumber("nodeCount"));
            assertEquals(2L, row.getNumber("colorCount"));
        });

        runQueryWithRowConsumer("MATCH (n) RETURN id(n) AS id, n.color AS color", row -> {
            long nodeId = row.getNumber("id").longValue();
            assertEquals(expectedResult.get(idToVariable.of(nodeId)), row.getNumber("color"));
        });
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
            .algo("gds", "k1coloring")
            .writeMode()
            .addParameter("writeProperty","foo2")
            .yields();


        var rowCount=runQueryWithRowConsumer(query, row -> {
            AssertionsForClassTypes.assertThat(row.getNumber("preProcessingMillis").longValue()).isNotEqualTo(-1);
            assertThat(row.getNumber("computeMillis").longValue()).isEqualTo(-1);
            assertThat(row.getNumber("nodeCount").longValue()).isEqualTo(0);
            assertThat(row.getNumber("colorCount").longValue()).isEqualTo(0);
            assertThat(row.getNumber("ranIterations").longValue()).isEqualTo(0);
            assertThat(row.getBoolean("didConverge")).isFalse();
        });

        assertThat(rowCount).isEqualTo(1L);
    }

}
