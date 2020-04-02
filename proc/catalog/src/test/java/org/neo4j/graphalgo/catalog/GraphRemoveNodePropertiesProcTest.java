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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;

class GraphRemoveNodePropertiesProcTest extends BaseProcTest {

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
        registerProcedures(GraphCreateProc.class, GraphRemoveNodePropertiesProc.class);
        runQuery(DB_CYPHER);

        runQuery(GdsCypher.call()
            .withAnyLabel()
            .withNodeProperty("nodeProp1")
            .withNodeProperty("nodeProp2")
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
    void removeNodeProperties() {
        String graphWriteQuery = String.format(
            "CALL gds.graph.removeNodeProperties(" +
            "   '%s', " +
            "   ['nodeProp1', 'nodeProp2']" +
            ") YIELD graphName, nodeProperties, propertiesRemoved",
            TEST_GRAPH_NAME
        );

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertEquals(TEST_GRAPH_NAME, row.getString("graphName"));
            assertEquals(Arrays.asList("nodeProp1", "nodeProp2"), row.get("nodeProperties"));
            assertEquals(12L, row.getNumber("propertiesRemoved").longValue());
        });
    }

    @Test
    void shouldFailOnNonExistingNodeProperty() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(String.format(
                "CALL gds.graph.removeNodeProperties(" +
                "   '%s', " +
                "   ['nodeProp1', 'nodeProp2', 'nodeProp3']" +
                ")",
                TEST_GRAPH_NAME
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("`nodeProp3` not found"));
        assertThat(rootCause.getMessage(), containsString("['nodeProp1', 'nodeProp2']"));
    }
}
