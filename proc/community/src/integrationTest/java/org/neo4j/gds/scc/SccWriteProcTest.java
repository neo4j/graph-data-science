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
package org.neo4j.gds.scc;

import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class SccWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (d:Node {name: 'd'})" +
        ", (e:Node {name: 'e'})" +
        ", (f:Node {name: 'f'})" +
        ", (g:Node {name: 'g'})" +
        ", (h:Node {name: 'h'})" +
        ", (i:Node {name: 'i'})" +

        ", (a)-[:TYPE {cost: 5}]->(b)" +
        ", (b)-[:TYPE {cost: 5}]->(c)" +
        ", (c)-[:TYPE {cost: 5}]->(a)" +

        ", (d)-[:TYPE {cost: 2}]->(e)" +
        ", (e)-[:TYPE {cost: 2}]->(f)" +
        ", (f)-[:TYPE {cost: 2}]->(d)" +

        ", (a)-[:TYPE {cost: 2}]->(d)" +

        ", (g)-[:TYPE {cost: 3}]->(h)" +
        ", (h)-[:TYPE {cost: 3}]->(i)" +
        ", (i)-[:TYPE {cost: 3}]->(g)";
    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            SccWriteProc.class
        );

        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME).graphProject().loadEverything(Orientation.NATURAL).yields();
        runQuery(projectQuery);
    }

    @Test
    void shouldWriteCorrectly() {

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.scc")
            .writeMode()
            .addParameter("writeProperty", "scc")
            .yields();

        runQueryWithRowConsumer(query, row -> {
            long preProcessingMillis = row.getNumber("preProcessingMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();

            assertThat(row.get("configuration")).isInstanceOf(Map.class)
                .asInstanceOf(MAP).containsAllEntriesOf(Map.of("writeProperty", "scc"));

            assertThat(row.get("componentDistribution"))
                .isInstanceOf(Map.class)
                .asInstanceOf(MAP)
                .containsExactlyInAnyOrderEntriesOf(
                    Map.ofEntries(
                        entry("p99", 3L),
                        entry("min", 3L),
                        entry("max", 3L),
                        entry("mean", 3.0),
                        entry("p999", 3L),
                        entry("p95", 3L),
                        entry("p90", 3L),
                        entry("p75", 3L),
                        entry("p50", 3L),
                        entry("p25", 3L),
                        entry("p10", 3L),
                        entry("p5", 3L),
                        entry("p1", 3L)

                    )
                );

            assertThat(row.getNumber("componentCount").longValue()).isEqualTo(3L);

            assertThat(preProcessingMillis).isNotEqualTo(-1L);
            assertThat(computeMillis).isNotEqualTo(-1L);
            assertThat(writeMillis).isNotEqualTo(-1L);

            assertThat(row.getNumber("nodePropertiesWritten")).isEqualTo(9L);

        });

        String validationQuery = "MATCH (n) RETURN n.scc as c";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        runQueryWithRowConsumer(validationQuery, row -> testMap.addTo(row.getNumber("c").intValue(), 1));

        // 3 sets with 3 elements each
        assertThat(testMap).hasSize(3);
        for (IntIntCursor cursor : testMap) {
            assertThat(cursor.value).isEqualTo(3);
        }

    }

    @Test
    void shouldUseConsecutiveIds() {

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.scc")
            .writeMode()
            .addParameter("consecutiveIds", true)
            .addParameter("writeProperty", "scc")
            .yields();

        runQueryWithRowConsumer(query, row -> {

            assertThat(row.get("configuration")).isInstanceOf(Map.class)
                .asInstanceOf(MAP).containsAllEntriesOf(Map.of("writeProperty", "scc"));

            assertThat(row.getNumber("nodePropertiesWritten")).isEqualTo(9L);

        });

        String validationQuery = "MATCH (n) RETURN n.scc as c";
        final Set<Long> testSet = new HashSet<>();
        runQueryWithRowConsumer(validationQuery, row -> testSet.add(row.getNumber("c").longValue()));
        assertThat(testSet).containsExactly(0L, 1L, 2L);

    }

    @Test
    void alphaShouldReturnCorrectly() {

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.alpha.scc")
            .writeMode()
            .yields();

        runQueryWithRowConsumer(query, row -> {
            long preProcessingMillis = row.getNumber("preProcessingMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();

            assertThat(row.getNumber("nodes").longValue()).isEqualTo(9L);

            assertThat(row.getNumber("p1").longValue()).isGreaterThan(0L);

            assertThat(row.getNumber("p99").longValue()).isEqualTo(3L);
            assertThat(row.getNumber("p95").longValue()).isEqualTo(3L);
            assertThat(row.getNumber("p90").longValue()).isEqualTo(3L);
            assertThat(row.getNumber("p75").longValue()).isEqualTo(3L);
            assertThat(row.getNumber("p50").longValue()).isEqualTo(3L);
            assertThat(row.getNumber("p25").longValue()).isEqualTo(3L);


            assertThat(row.getNumber("setCount").longValue()).isEqualTo(3L);
            assertThat(row.getNumber("minSetSize").longValue()).isEqualTo(3L);
            assertThat(row.getNumber("maxSetSize").longValue()).isEqualTo(3L);

            assertThat(row.getString("writeProperty")).isEqualTo("componentId");
            assertThat(preProcessingMillis).isNotEqualTo(-1L);
            assertThat(computeMillis).isNotEqualTo(-1L);
            assertThat(writeMillis).isNotEqualTo(-1L);

        });

        String validationQuery = "MATCH (n) RETURN n.componentId as c";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        runQueryWithRowConsumer(validationQuery, row -> testMap.addTo(row.getNumber("c").intValue(), 1));

        // 3 sets with 3 elements each
        assertThat(testMap).hasSize(3);
        for (IntIntCursor cursor : testMap) {
            assertThat(cursor.value).isEqualTo(3);
        }
    }

}
