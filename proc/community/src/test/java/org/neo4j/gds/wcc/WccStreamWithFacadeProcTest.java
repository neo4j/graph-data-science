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
package org.neo4j.gds.wcc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;

class WccStreamWithFacadeProcTest extends BaseProcTest {

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE" +
            " (nA:Label {nodeId: 0, seedId: 42})" +
            ",(nB:Label {nodeId: 1, seedId: 42})" +
            ",(nC:Label {nodeId: 2, seedId: 42})" +
            ",(nD:Label {nodeId: 3, seedId: 42})" +
            ",(nE:Label2 {nodeId: 4})" +
            ",(nF:Label2 {nodeId: 5})" +
            ",(nG:Label2 {nodeId: 6})" +
            ",(nH:Label2 {nodeId: 7})" +
            ",(nI:Label2 {nodeId: 8})" +
            ",(nJ:Label2 {nodeId: 9})" +
            // {A, B, C, D}
            ",(nA)-[:TYPE]->(nB)" +
            ",(nB)-[:TYPE]->(nC)" +
            ",(nC)-[:TYPE]->(nD)" +
            ",(nD)-[:TYPE {cost:4.2}]->(nE)" + // threshold UF should split here
            // {E, F, G}
            ",(nE)-[:TYPE]->(nF)" +
            ",(nF)-[:TYPE]->(nG)" +
            // {H, I}
            ",(nH)-[:TYPE]->(nI)";

    private long[][] EXPECTED_COMMUNITIES;

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            WccStreamWithFacadeProc.class,
            GraphProjectProc.class
        );

        EXPECTED_COMMUNITIES = new long[][]{
            idFunction.of("nA", "nB", "nC", "nD", "nE", "nF", "nG"),
            idFunction.of("nH", "nI"),
            new long[]{
                idFunction.of("nJ")
            }
        };
    }

    @AfterEach
    void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldStream() {

        runQuery("CALL gds.graph.project('graph', '*', '*')");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .streamMode()
            .addParameter("minComponentSize", 1)
            .yields("nodeId", "componentId");

        var resultRowCount = runQueryWithRowConsumer("CALL gds.wcc.facade.stream('graph', {}) YIELD nodeId, componentId", row -> {});

        assertThat(resultRowCount).isEqualTo(10);
    }


}
