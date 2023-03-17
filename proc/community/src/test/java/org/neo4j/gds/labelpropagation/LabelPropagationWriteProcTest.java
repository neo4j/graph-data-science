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
package org.neo4j.gds.labelpropagation;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class LabelPropagationWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {id: 0, seed: 42}) " +
        ", (b:B {id: 1, seed: 42}) " +

        ", (a)-[:X]->(:A {id: 2,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 3,  weight: 2.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 4,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 5,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 6,  weight: 8.0, seed: 2}) " +

        ", (b)-[:X]->(:B {id: 7,  weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 8,  weight: 2.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 9,  weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 10, weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 11, weight: 8.0, seed: 2})";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            LabelPropagationWriteProc.class,
            GraphProjectProc.class
        );
        // Create explicit graphs with both projection variants
        runQuery(
            "CALL gds.graph.project(" +
            "   'myGraph', " +
            "   {" +
            "       A: {label: 'A', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}, " +
            "       B: {label: 'B', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}" +
            "   }, " +
            "   '*'" +
            ")"
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testWrite() {
        var query = "CALL gds.labelPropagation.write('myGraph', {writeProperty: 'myFancyCommunity'})" +
                    " YIELD communityCount, preProcessingMillis, computeMillis, writeMillis, " +
                    " postProcessingMillis, communityDistribution, didConverge";

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(10);

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("writeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getBoolean("didConverge")).isTrue();

            assertThat(row.get("communityDistribution"))
                .isInstanceOf(Map.class)
                .asInstanceOf(MAP)
                .containsKeys("min", "max", "mean", "p50", "p75", "p90", "p99", "p999", "p95");
        });

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);
    }

    // FIXME: This doesn't belong here.
    @Test
    void respectsMaxIterations() {
        @Language("Cypher") String query = GdsCypher.call("myGraph")
            .algo("labelPropagation")
            .writeMode()
            .addParameter("writeProperty", "label")
            .addParameter("maxIterations", 5)
            .yields("ranIterations", "communityDistribution");

        var rowCount = runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(row.getNumber("ranIterations"))
                    .asInstanceOf(LONG)
                    .isLessThanOrEqualTo(5);
                assertThat(row.get("communityDistribution"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(MAP)
                    .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "p999", 2L,
                            "p99", 2L,
                            "p95", 2L,
                            "p90", 2L,
                            "p75", 1L,
                            "p50", 1L,
                            "min", 1L,
                            "max", 2L,
                            "mean", 1.2D
                        )
                    );
            }
        );

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);
    }

    @ParameterizedTest(name = "concurrency = {0}")
    @ValueSource(ints = {1, 2, 4})
    void shouldRunLabelPropagationWithIdenticalSeedAndWriteProperties(int concurrency) {

        String query = GdsCypher.call("myGraph")
            .algo("gds.labelPropagation")
            .writeMode()
            .addParameter("concurrency", concurrency)
            .addParameter("writeProperty", "seed")
            .addParameter("seedProperty", "seed")
            .addParameter("nodeWeightProperty", "weight")
            .yields();

        var rowCount = runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(2L);

                assertUserInput(row, "seedProperty", "seed");
                assertUserInput(row, "writeProperty", "seed");
                assertThat(row.getNumber("preProcessingMillis"))
                    .asInstanceOf(LONG)
                    .as("load time not set")
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("computeMillis"))
                    .asInstanceOf(LONG)
                    .as("compute time not set")
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("writeMillis"))
                    .asInstanceOf(LONG)
                    .as("write time not set")
                    .isGreaterThan(-1L);
                assertThat(row.get("communityDistribution"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(MAP)
                    .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "p999", 8L,
                            "p99", 8L,
                            "p95", 8L,
                            "p90", 8L,
                            "p75", 8L,
                            "p50", 4L,
                            "min", 4L,
                            "max", 8L,
                            "mean", 6.0D
                        )
                    );
            }
        );

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);

        String check = "MATCH (n) " +
                       "WHERE n.id IN [0,1] " +
                       "RETURN n.seed AS community";
        runQueryWithRowConsumer(
            check,
            row ->
                assertThat(row.getNumber("community"))
                    .asInstanceOf(LONG)
                    .isEqualTo(2L)
        );
    }

    @ParameterizedTest(name = "concurrency = {0}")
    @ValueSource(ints = {1, 2, 4})
    void shouldRunLabelPropagationWithoutInitialSeed(int concurrency) {

        String query = GdsCypher.call("myGraph")
            .algo("gds.labelPropagation")
            .writeMode()
            .addParameter("concurrency", concurrency)
            .addParameter("writeProperty", "community")
            .addParameter("nodeWeightProperty", "weight")
            .yields();

        var rowCount = runQueryWithRowConsumer(
            query,
            row -> {
                assertUserInput(row, "seedProperty", null);
                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(12L);

                assertThat(row.getNumber("preProcessingMillis"))
                    .asInstanceOf(LONG)
                    .as("load time not set")
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("computeMillis"))
                    .asInstanceOf(LONG)
                    .as("compute time not set")
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("writeMillis"))
                    .asInstanceOf(LONG)
                    .as("write time not set")
                    .isGreaterThan(-1L);
            }
        );

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);

        runQueryWithRowConsumer(
            "MATCH (n) WHERE n.id = 0 RETURN n.community AS community",
            row ->
                assertThat(row.getNumber("community"))
                    .asInstanceOf(LONG)
                    .isEqualTo(6L)

        );
        runQueryWithRowConsumer(
            "MATCH (n) WHERE n.id = 1 RETURN n.community AS community",
            row ->
                assertThat(row.getNumber("community"))
                    .asInstanceOf(LONG)
                    .isEqualTo(11L)
        );
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
            Arguments.of(Map.of("minCommunitySize", 1), List.of(2L, 7L, 3L, 4L, 5L, 6L, 8L, 9L, 10L, 11L)),
            Arguments.of(Map.of("minCommunitySize", 2), List.of(2L, 7L)),
            Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)),
            Arguments.of(Map.of("minCommunitySize", 2, "consecutiveIds", true), List.of(0L, 1L)),
            Arguments.of(Map.of("minCommunitySize", 1, "seedProperty", "seed"), List.of(1L, 2L)),
            Arguments.of(Map.of("minCommunitySize", 2, "seedProperty", "seed"), List.of(1L, 2L))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteWithMinCommunitySize(Map<String, Object> parameters, Iterable<Long> expectedCommunityIds) {
        String writeProp = "writeProp";
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withNodeProperty("seed")
            .yields();
        runQuery(createQuery);

        var query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("labelPropagation")
            .writeMode()
            .addParameter("writeProperty", writeProp)
            .addAllParameters(parameters)
            .yields("communityCount");

        var rowCount = runQueryWithRowConsumer(
            query,
            row ->
                assertThat(row.getNumber("communityCount"))
                    .asInstanceOf(LONG)
                    .isEqualTo(parameters.containsKey("seedProperty") ? 2L : 10L)

        );

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);

        runQueryWithRowConsumer(
            "MATCH (n) RETURN collect(DISTINCT n." + writeProp + ") AS communities ",
            row ->
                assertThat(row.get("communities"))
                    .asList()
                    .containsExactlyInAnyOrderElementsOf(expectedCommunityIds)
        );
    }

    @Test
    void testWriteEstimate() {
        var query = "CALL gds.labelPropagation.write.estimate('myGraph', {writeProperty: 'foo', concurrency: 4})" +
                    " YIELD bytesMin, bytesMax, nodeCount, relationshipCount";

        assertCypherResult(query, List.of(Map.of(
            "nodeCount", 12L,
            "relationshipCount", 10L,
            "bytesMin", 1640L,
            "bytesMax", 2152L
        )));
    }

    // FIXME: This doesn't belong here.
    @Test
    void zeroCommunitiesInEmptyGraph() {
        runQuery("CALL db.createLabel('VeryTemp')");
        runQuery("CALL db.createRelationshipType('VERY_TEMP')");

        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .yields();
        runQuery(createQuery);

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("labelPropagation")
            .writeMode()
            .addParameter("writeProperty", "foo")
            .yields("communityCount");

        assertCypherResult(query, List.of(Map.of("communityCount", 0L)));
    }

    @Nested
    class FilteredGraph extends BaseTest {

        @Neo4jGraph(offsetIds = true)
        static final String DB_CYPHER_WITH_OFFSET = DB_CYPHER;

        @Test
        void shouldRunLabelPropagationNaturalOnFilteredNodes() {

            String query = GdsCypher.call("myGraph")
                .algo("gds.labelPropagation")
                .writeMode()
                .addParameter("writeProperty", "community")
                .addParameter("seedProperty", "seed")
                .addParameter("nodeWeightProperty", "weight")
                .addParameter("nodeLabels", Arrays.asList("A", "B"))
                .yields();

            var rowCount = runQueryWithRowConsumer(
                query,
                row -> {
                    assertThat(row.getNumber("nodePropertiesWritten"))
                        .asInstanceOf(LONG)
                        .isEqualTo(12L);

                    assertThat(row.getNumber("preProcessingMillis"))
                        .asInstanceOf(LONG)
                        .as("load time not set")
                        .isGreaterThan(-1L);

                    assertThat(row.getNumber("computeMillis"))
                        .asInstanceOf(LONG)
                        .as("compute time not set")
                        .isGreaterThan(-1L);

                    assertThat(row.getNumber("writeMillis"))
                        .asInstanceOf(LONG)
                        .as("write time not set")
                        .isGreaterThan(-1L);

                    assertThat(row.get("communityDistribution"))
                        .isInstanceOf(Map.class)
                        .asInstanceOf(MAP)
                        .containsExactlyInAnyOrderEntriesOf(
                            Map.of(
                                "p999", 8L,
                                "p99", 8L,
                                "p95", 8L,
                                "p90", 8L,
                                "p75", 8L,
                                "p50", 4L,
                                "min", 4L,
                                "max", 8L,
                                "mean", 6.0D
                            )
                        );
                }
            );

            assertThat(rowCount)
                .as("`write` mode should always return one row")
                .isEqualTo(1);

            String check = "MATCH (n) " +
                           "WHERE n.id IN [0,1] " +
                           "RETURN n.community AS community";
            runQueryWithRowConsumer(
                check,
                row ->
                    assertThat(row.getNumber("community"))
                        .asInstanceOf(LONG)
                        .isEqualTo(2L)
            );
        }
    }

    // FIXME: This doesn't belong here.
    @Test
    void shouldRunLabelPropagationNatural() {

        String query = GdsCypher.call("myGraph")
            .algo("gds.labelPropagation")
            .writeMode()
            .addParameter("writeProperty", "community")
            .addParameter("seedProperty", "seed")
            .addParameter("nodeWeightProperty", "weight")
            .yields();

        var rowCount = runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(12L);
                assertThat(row.getNumber("preProcessingMillis"))
                    .asInstanceOf(LONG)
                    .as("load time not set")
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("computeMillis"))
                    .asInstanceOf(LONG)
                    .as("compute time not set")
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("writeMillis"))
                    .asInstanceOf(LONG)
                    .as("write time not set")
                    .isGreaterThan(-1L);
                assertThat(row.get("communityDistribution"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(MAP)
                    .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "p999", 8L,
                            "p99", 8L,
                            "p95", 8L,
                            "p90", 8L,
                            "p75", 8L,
                            "p50", 4L,
                            "min", 4L,
                            "max", 8L,
                            "mean", 6.0D
                        )
                    );
            }
        );

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);

        String check = "MATCH (n) " +
                       "WHERE n.id IN [0,1] " +
                       "RETURN n.community AS community";
        runQueryWithRowConsumer(check, row ->
            assertThat(row.getNumber("community"))
                .asInstanceOf(LONG)
                .isEqualTo(2L));
    }

    // FIXME: This doesn't belong here.
    @Test
    void shouldRunLabelPropagationUndirected() {
        runQuery(
            "CALL gds.graph.project(" +
            "   'undirected', " +
            "   {" +
            "       A: {label: 'A', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}, " +
            "       B: {label: 'B', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}" +
            "   }, " +
            "   {" +
            "       X: { type: 'X', orientation: 'UNDIRECTED' }" +
            "   }" +
            ")"

        );

        var query = "CALL gds.labelPropagation.write(" +
                    "        'undirected', {" +
                    "         writeProperty: 'community'" +
                    "    }" +
                    ")";

        var rowCount = runQueryWithRowConsumer(query, row -> {
                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(12L);
                assertThat(row.get("communityDistribution"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(MAP)
                    .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "p999", 6L,
                            "p99", 6L,
                            "p95", 6L,
                            "p90", 6L,
                            "p75", 6L,
                            "p50", 6L,
                            "min", 6L,
                            "max", 6L,
                            "mean", 6.0D
                        )
                    );
            }
        );

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);

        var check = formatWithLocale("MATCH (a {id: 0}), (b {id: 1}) " +
                                     "RETURN a.%1$s AS a, b.%1$s AS b", "community");
        runQueryWithRowConsumer(check, row -> {
            assertThat(row.getNumber("a"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
            assertThat(row.getNumber("b"))
                .asInstanceOf(LONG)
                .isEqualTo(7);
        });
    }

    // FIXME: This doesn't belong here.
    @Test
    void shouldRunLabelPropagationReverse() {
        runQuery(
            "CALL gds.graph.project(" +
            "   'reverse', " +
            "   {" +
            "       A: {label: 'A', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}, " +
            "       B: {label: 'B', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}" +
            "   }, " +
            "   {" +
            "       X: { type: 'X', orientation: 'REVERSE' }" +
            "   }" +
            ")"
        );

        var query = "CALL gds.labelPropagation.write(" +
                    "        'reverse', {" +
                    "         writeProperty: 'community'" +
                    "    }" +
                    ")";

        var rowCount = runQueryWithRowConsumer(query,
            row -> {
                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(12L);

                assertThat(row.getNumber("preProcessingMillis"))
                    .asInstanceOf(LONG)
                    .as("load time not set")
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("computeMillis"))
                    .asInstanceOf(LONG)
                    .as("compute time not set")
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("writeMillis"))
                    .asInstanceOf(LONG)
                    .as("write time not set")
                    .isGreaterThan(-1L);
                assertThat(row.get("communityDistribution"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(MAP)
                    .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "p999", 6L,
                            "p99", 6L,
                            "p95", 6L,
                            "p90", 6L,
                            "p75", 6L,
                            "p50", 6L,
                            "min", 6L,
                            "max", 6L,
                            "mean", 6.0D
                        )
                    );
            }
        );

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);

        String validateQuery = formatWithLocale(
            "MATCH (n) RETURN n.%1$s AS community, count(*) AS communitySize",
            "community"
        );
        assertCypherResult(validateQuery, Arrays.asList(
            Map.of("community", 0L, "communitySize", 6L),
            Map.of("community", 1L, "communitySize", 6L)
        ));
    }
}
