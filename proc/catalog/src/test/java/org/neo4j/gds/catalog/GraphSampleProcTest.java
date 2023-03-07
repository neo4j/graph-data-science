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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.pagerank.PageRankMutateProc;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class GraphSampleProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (x:Z {prop: 42})" +
        ", (x1:Z {prop: 43})" +
        ", (x2:Z {prop: 44})" +
        ", (x3:Z {prop: 45})" +
        ", (a:N {prop: 46})" +
        ", (b:N {prop: 47})" +
        ", (c:N {prop: 48, attr: 48})" +
        ", (d:N {prop: 49, attr: 48})" +
        ", (e:M {prop: 50, attr: 48})" +
        ", (f:M {prop: 51, attr: 48})" +
        ", (g:M {prop: 52})" +
        ", (h:M {prop: 53})" +
        ", (i:X {prop: 54})" +
        ", (j:M {prop: 55})" +
        ", (x)-[:R1]->(x1)" +
        ", (x)-[:R1]->(x2)" +
        ", (x)-[:R1]->(x3)" +
        ", (e)-[:R1]->(d)" +
        ", (i)-[:R1]->(g)" +
        ", (a)-[:R1 {cost: 10.0, distance: 5.8}]->(b)" +
        ", (a)-[:R1 {cost: 10.0, distance: 4.8}]->(c)" +
        ", (c)-[:R1 {cost: 10.0, distance: 5.8}]->(d)" +
        ", (d)-[:R1 {cost:  4.2, distance: 2.6}]->(e)" +
        ", (e)-[:R1 {cost: 10.0, distance: 5.8}]->(f)" +
        ", (f)-[:R1 {cost: 10.0, distance: 9.9}]->(g)" +
        ", (h)-[:R2 {cost: 10.0, distance: 5.8}]->(i)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphSampleProc.class,
            GraphListProc.class,
            PageRankMutateProc.class,
            GraphStreamNodePropertiesProc.class
        );
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldSample() {
        runQuery("CALL gds.graph.project('g', '*', '*')");

        var query =
            "CALL gds.alpha.graph.sample.rwr('sample', 'g', {samplingRatio: 1.0}) YIELD nodeCount";
        assertCypherResult(query, List.of(
            Map.of("nodeCount", 14L)
        ));
        assertGraphExists("sample");
    }

    @ParameterizedTest
    @CsvSource(value = {"0.28,1", "0.35,2"})
    void shouldUseSingleStartNode(double samplingRatio, long expectedStartNodeCount) {
        runQuery("CALL gds.graph.project('g', '*', '*')");

        var query =
            "MATCH (z:Z {prop: 42})" +
            " CALL gds.alpha.graph.sample.rwr('sample', 'g', {samplingRatio: $samplingRatio, startNodes: [id(z)], concurrency: 1, randomSeed: 42} )" +
            " YIELD startNodeCount RETURN startNodeCount";
        assertCypherResult(query, Map.of("samplingRatio", samplingRatio), List.of(
            Map.of("startNodeCount", expectedStartNodeCount)
        ));
        assertGraphExists("sample");
    }

    @Test
    void shouldSampleHalf() {
        runQuery("CALL gds.graph.project('g', '*', '*')");

        var query =
            "CALL gds.alpha.graph.sample.rwr('sample', 'g', {samplingRatio: 0.5, concurrency: 1, randomSeed: 42}) YIELD nodeCount";
        assertCypherResult(query, List.of(
            Map.of("nodeCount", 7L)
        ));
        assertGraphExists("sample");
    }

    @Test
    void shouldListCorrectGraphProjectionConfig() {
        runQuery("CALL gds.graph.project('g', ['Z', 'N'], ['R1'])");

        runQuery("CALL gds.alpha.graph.sample.rwr('sample', 'g', {samplingRatio: 0.5})");
        assertGraphExists("sample");
        runQueryWithRowConsumer("CALL gds.graph.list('sample') YIELD configuration", resultRow -> {
            assertThat(resultRow.get("configuration"))
                .isInstanceOf(Map.class)
                .asInstanceOf(MAP)
                .containsEntry("samplingRatio", 0.5);
        });
    }

    @Test
    void shouldMutateCorrectGraph() {
        long expectedNodeCount = 7L;

        runQuery("CALL gds.graph.project('g', '*', '*')");

        var query =
            "CALL gds.alpha.graph.sample.rwr('sample', 'g', {samplingRatio: 0.5, concurrency: 1, randomSeed: 42}) YIELD nodeCount";
        assertCypherResult(query, List.of(
            Map.of("nodeCount", expectedNodeCount)
        ));
        assertGraphExists("sample");

        assertCypherResult(
            "CALL gds.pageRank.mutate('sample', {mutateProperty: 'rank'}) YIELD nodePropertiesWritten",
            List.of(
                Map.of("nodePropertiesWritten", expectedNodeCount)
            )
        );

        var numberOfStreamedProperties = runQueryWithRowConsumer(
            "CALL gds.graph.nodeProperty.stream('sample', 'rank')",
            unused -> {}
        );
        assertThat(numberOfStreamedProperties).isEqualTo(expectedNodeCount);

        assertThatThrownBy(() -> runQuery("CALL gds.graph.nodeProperty.stream('g', 'rank')"))
            .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }
}
