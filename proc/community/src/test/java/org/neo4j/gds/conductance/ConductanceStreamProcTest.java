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
package org.neo4j.gds.conductance;

import org.assertj.core.data.Offset;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConductanceStreamProcTest extends BaseProcTest {

    private static final String GRAPH_NAME = "myGraph";

    @Neo4jGraph
    @Language("Cypher")
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label1 { community: 0 })" +
        ", (b:Label1 { community: 0 })" +
        ", (c:Label1 { community: 0 })" +
        ", (d:Label1 { community: 1 })" +
        ", (e:Label1 { community: 1 })" +
        ", (f:Label1 { community: 1 })" +
        ", (g:Label1 { community: 1 })" +
        ", (h:Label1 { community: -1 })" +

        ", (a)-[:TYPE1 {weight: 81.0}]->(b)" +
        ", (a)-[:TYPE1 {weight: 7.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(g)" +
        ", (b)-[:TYPE1 {weight: 3.0}]->(h)" +
        ", (c)-[:TYPE1 {weight: 45.0}]->(b)" +
        ", (c)-[:TYPE1 {weight: 3.0}]->(e)" +
        ", (d)-[:TYPE1 {weight: 3.0}]->(c)" +
        ", (e)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 3.0}]->(a)" +
        ", (g)-[:TYPE1 {weight: 4.0}]->(c)" +
        ", (g)-[:TYPE1 {weight: 999.0}]->(g)" +
        ", (h)-[:TYPE1 {weight: 2.0}]->(a)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            ConductanceStreamProc.class,
            GraphProjectProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeProperty("community")
            .loadEverything()
            .yields();

        runQuery(createQuery);
    }

    @AfterEach
    void clearStore() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStream() {
        String streamQuery = GdsCypher.call(GRAPH_NAME)
            .algo("conductance")
            .streamMode()
            .addParameter("communityProperty", "community")
            .yields();

        var expected = Map.of(
            0L, 5.0 / (5.0 + 2.0),
            1L, 4.0 / (4.0 + 1.0)
        );
        runQueryWithRowConsumer(streamQuery, row -> {
            long community = row.getNumber("community").longValue();
            double conductance = row.getNumber("conductance").doubleValue();
            assertThat(conductance).isCloseTo(expected.get(community), Offset.offset(0.0001));
        });

    }
}
