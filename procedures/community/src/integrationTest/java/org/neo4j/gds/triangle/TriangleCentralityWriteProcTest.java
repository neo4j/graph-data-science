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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class TriangleCentralityWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
        "(a:A {name: 'a'})-[:T]->(b:A {name: 'b'}), " +
        "(b)-[:T]->(c:A {name: 'c'}), (c)-[:T]->(a), " +
        "(a)-[:T]->(d:A {name: 'd'}), (b)-[:T]->(d), (c)-[:T]->(d), " +
        "(a)-[:T]->(e:A {name: 'e'}), (b)-[:T]->(e)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, TriangleCentralityWriteProc.class);
        runQuery("CALL gds.graph.project('graph', 'A', {T: {orientation: 'UNDIRECTED'}})");
    }

    @Test
    void shouldWrite() {
        write(Long.MAX_VALUE, 5);

        assertWriteResult(Map.of("a", 1.0, "b", 1.0, "c", 14.0 / 15, "d", 14.0 / 15, "e", 0.6));
    }

    @Test
    void shouldWriteWithMaxDegree() {
        write(2, 1);

        // Excluded nodes have NaN scores, which the standard GDS writer skips.
        assertWriteResult(Map.of("e", 0.0));
    }

    @Test
    void shouldPreserveExistingPropertiesForExcludedNodes() {
        write(Long.MAX_VALUE, 5);
        write(2, 1);

        assertWriteResult(Map.of("a", 1.0, "b", 1.0, "c", 14.0 / 15, "d", 14.0 / 15, "e", 0.0));
    }

    private void write(long maxDegree, long expectedPropertiesWritten) {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("triangleCentrality")
            .writeMode()
            .addParameter("writeProperty", "centrality")
            .addParameter("maxDegree", maxDegree)
            .yields();

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("globalTriangleCount")).asInstanceOf(LONG)
                .isEqualTo(maxDegree == Long.MAX_VALUE ? 5L : 0L);
            assertThat(row.getNumber("nodeCount")).asInstanceOf(LONG).isEqualTo(5);
            assertThat(row.getNumber("nodePropertiesWritten")).asInstanceOf(LONG).isEqualTo(expectedPropertiesWritten);
            assertThat(row.getNumber("preProcessingMillis")).asInstanceOf(LONG).isNotNegative();
            assertThat(row.getNumber("computeMillis")).asInstanceOf(LONG).isNotNegative();
            assertThat(row.getNumber("postProcessingMillis")).asInstanceOf(LONG).isNotNegative();
            assertThat(row.getNumber("writeMillis")).asInstanceOf(LONG).isNotNegative();
            assertThat(row.get("configuration")).isInstanceOf(Map.class);
        });
        assertThat(rowCount).isEqualTo(1);
    }

    private void assertWriteResult(Map<String, Double> expected) {
        var rowCount = runQueryWithRowConsumer("MATCH (n:A) RETURN n.name AS name, n.centrality AS centrality", row -> {
            Double centrality = expected.get(row.getString("name"));
            if (centrality == null) {
                assertThat(row.get("centrality")).isNull();
            } else {
                assertThat(row.getNumber("centrality")).asInstanceOf(DOUBLE).isCloseTo(centrality, within(1e-10));
            }
        });
        assertThat(rowCount).isEqualTo(5);
    }
}
