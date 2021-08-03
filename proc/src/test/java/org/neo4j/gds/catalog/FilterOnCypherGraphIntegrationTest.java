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
import org.neo4j.gds.labelpropagation.LabelPropagationStatsProc;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;

class FilterOnCypherGraphIntegrationTest extends BaseProcTest {

    private static final String TEST_GRAPH = "testGraph";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node { nodeId: 0 })" +
        ", (b:Node { nodeId: 1 })" +
        ", (c:Node { nodeId: 2 })" +
        ", (d:Node { nodeId: 3 })" +
        ", (e:Node { nodeId: 4 })" +
        ", (f:Node { nodeId: 5 })" +
        ", (g:Node2 { nodeId: 6 })" +
        ", (h:Node2 { nodeId: 7 })" +
        ", (i:Node2 { nodeId: 8 })" +
        ", (j:Node2 { nodeId: 9 })" +
        ", (k:Node2 { nodeId: 10 })" +
        ", (l:Node2 { nodeId: 11 })" +
        ", (a)-[:TYPE {p: 10}]->(b)" +
        ", (b)-[:TYPE2]->(c)" +
        ", (c)-[:TYPE2]->(d)" +
        ", (d)-[:TYPE2]->(e)" +
        ", (e)-[:TYPE2]->(f)" +
        ", (f)-[:TYPE]->(g)" +
        ", (h)-[:TYPE]->(i)" +
        ", (i)-[:TYPE]->(k)" +
        ", (i)-[:TYPE]->(l)" +
        ", (j)-[:TYPE]->(k)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, LabelPropagationStatsProc.class);
        runQuery(DB_CYPHER);

        runQuery("CALL gds.graph.create.cypher($graphName, $nodeQuery, $relQuery)",
            map("graphName", TEST_GRAPH, "nodeQuery", "MATCH (n) RETURN id(n) AS id, labels(n) AS labels", "relQuery", ALL_RELATIONSHIPS_QUERY));
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldFilterOnACypherGraph() {
        GdsCypher.ParametersBuildStage labelPropagationQuery = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH)
            .algo("labelPropagation")
            .statsMode();

        String unfilteredQuery = labelPropagationQuery.yields("communityCount");
        String filteredQuery = labelPropagationQuery.addParameter("nodeLabels", Arrays.asList("Node")).yields("communityCount");

        runQueryWithResultConsumer(filteredQuery,
            result -> runQueryWithResultConsumer(
                unfilteredQuery,
                unfilteredResult -> {
                    assertNotEquals(result.resultAsString(), unfilteredResult.resultAsString());
                }
            ));
    }
}
