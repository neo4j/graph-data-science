/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.catalog;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Arrays;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;

class GraphWriteNodePropertiesProcTest extends BaseProcTest {

    private static final String TEST_GRAPH_NAME = "testGraph";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {nodeProp1: 0, nodeProp2: 42})" +
        ", (b:Node {nodeProp1: 1, nodeProp2: 43})" +
        ", (c:Node {nodeProp1: 2, nodeProp2: 44})" +
        ", (d:Node {nodeProp1: 3, nodeProp2: 45})" +
        ", (e:Node {nodeProp1: 4, nodeProp2: 46})" +
        ", (f:Node {nodeProp1: 5, nodeProp2: 47})";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphCreateProc.class, GraphWriteNodePropertiesProc.class);
        runQuery(DB_CYPHER);

        runQuery(GdsCypher.call()
            .withAnyLabel()
            .withNodeProperty("newNodeProp1", "nodeProp1")
            .withNodeProperty("newNodeProp2", "nodeProp2")
            .withAnyRelationshipType()
            .graphCreate(TEST_GRAPH_NAME)
            .yields()
        );
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void writeNodeProperties() {
        String graphWriteQuery = String.format(
            "CALL gds.graph.writeNodeProperties(" +
            "   '%s', " +
            "   ['newNodeProp1', 'newNodeProp2']" +
            ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten",
            TEST_GRAPH_NAME
        );

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertThat(-1L, Matchers.lessThan(row.getNumber("writeMillis").longValue()));
            assertEquals(TEST_GRAPH_NAME, row.getString("graphName"));
            assertEquals(Arrays.asList("newNodeProp1", "newNodeProp2"), row.get("nodeProperties"));
            assertEquals(12L, row.getNumber("propertiesWritten").longValue());
        });

        String validationQuery =
            "MATCH (n) " +
            "RETURN " +
            "  n.newNodeProp1 AS newProp1, " +
            "  n.newNodeProp2 AS newProp2 " +
            "ORDER BY newProp1 ASC, newProp2 ASC";

        assertCypherResult(validationQuery, asList(
            map("newProp1", 0D, "newProp2", 42D),
            map("newProp1", 1D, "newProp2", 43D),
            map("newProp1", 2D, "newProp2", 44D),
            map("newProp1", 3D, "newProp2", 45D),
            map("newProp1", 4D, "newProp2", 46D),
            map("newProp1", 5D, "newProp2", 47D)
        ));
    }

    @Test
    void shouldFailOnNonExistingNodeProperty() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(String.format(
                "CALL gds.graph.writeNodeProperties(" +
                "   '%s', " +
                "   ['newNodeProp1', 'newNodeProp2', 'newNodeProp3']" +
                ")",
                TEST_GRAPH_NAME
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("No node projection with property key `newNodeProp3` found"));
        assertThat(rootCause.getMessage(), containsString("[newNodeProp1, newNodeProp2]"));
    }
}
