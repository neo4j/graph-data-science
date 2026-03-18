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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.GdlSupport.fromGdl;
import static org.neo4j.gds.TestSupport.assertGraphEquals;

class TriangleCentralityMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
        "(a:A)-[:T]->(b:A), (b)-[:T]->(c:A), (c)-[:T]->(a), (a)-[:T]->(d:A)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, TriangleCentralityMutateProc.class);
        runQuery("CALL gds.graph.project('graph', 'A', {T: {orientation: 'UNDIRECTED'}})");
    }

    @Test
    void shouldMutateYield() {
        mutate(Long.MAX_VALUE);

        assertGraphEquals(fromGdl(
            "(a:A {centrality: 1.0}), (b:A {centrality: 1.0}), " +
                "(c:A {centrality: 1.0}), (d:A {centrality: 1.0}), " +
                // Graph is UNDIRECTED, so each relationship appears twice.
                "(a)-[:T]->(b), (b)-[:T]->(a), (b)-[:T]->(c), (c)-[:T]->(b), " +
                "(c)-[:T]->(a), (a)-[:T]->(c), (a)-[:T]->(d), (d)-[:T]->(a)"
        ), mutatedGraph());
    }

    @Test
    void shouldMutateWithMaxDegree() {
        mutate(2);

        var graph = mutatedGraph();
        var values = graph.nodeProperties("centrality");
        assertThat(values.doubleValue(graph.toMappedNodeId(idFunction.of("a")))).isNaN();
        for (String node : List.of("b", "c", "d")) {
            assertThat(values.doubleValue(graph.toMappedNodeId(idFunction.of(node)))).isZero();
        }
    }

    private void mutate(long maxDegree) {
        runQueryWithRowConsumer(
            "CALL gds.triangleCentrality.mutate('graph', {mutateProperty: 'centrality', maxDegree: $maxDegree})",
            Map.of("maxDegree", maxDegree),
            row -> {
                assertThat(row.getNumber("globalTriangleCount")).asInstanceOf(LONG)
                    .isEqualTo(maxDegree == Long.MAX_VALUE ? 1L : 0L);
                assertThat(row.getNumber("nodeCount")).asInstanceOf(LONG).isEqualTo(4);
                assertThat(row.getNumber("nodePropertiesWritten")).asInstanceOf(LONG).isEqualTo(4);
                assertThat(row.getNumber("preProcessingMillis")).asInstanceOf(LONG).isNotNegative();
                assertThat(row.getNumber("computeMillis")).asInstanceOf(LONG).isNotNegative();
                assertThat(row.getNumber("postProcessingMillis")).asInstanceOf(LONG).isNotNegative();
                assertThat(row.getNumber("mutateMillis")).asInstanceOf(LONG).isNotNegative();
                assertThat(row.get("configuration")).isInstanceOf(Map.class);
            }
        );
    }

    private Graph mutatedGraph() {
        return GraphStoreCatalog.get(getUsername(), DatabaseId.of(db.databaseName()), "graph").graphStore().getUnion();
    }
}
