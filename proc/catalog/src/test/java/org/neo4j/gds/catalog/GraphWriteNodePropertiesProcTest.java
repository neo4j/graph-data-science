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

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphWriteNodePropertiesProcTest extends BaseProcTest {
    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";

    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
            ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
            ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
            ", (d:B {nodeProp1: 3, nodeProp2: 45})" +
            ", (e:B {nodeProp1: 4, nodeProp2: 46})" +
            ", (f:B {nodeProp1: 5, nodeProp2: 47})";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
        runQuery(DB_CYPHER);

        runQuery(GdsCypher.call(TEST_GRAPH_SAME_PROPERTIES)
            .graphProject()
            .withNodeLabels("A", "B")
            .withNodeProperty("newNodeProp1", "nodeProp1")
            .withNodeProperty("newNodeProp2", "nodeProp2")
            .withAnyRelationshipType()
            .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldWriteNodeProperties() {
        var graphWriteQuery = formatWithLocale(
            "CALL gds.graph.nodeProperties.write('%s', 'newNodeProp1') " +
                "YIELD writeMillis, propertiesWritten, graphName, nodeProperties",
            TEST_GRAPH_SAME_PROPERTIES
        );
        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertThat(-1L, Matchers.lessThan(row.getNumber("writeMillis").longValue()));
            assertEquals(6L, row.getNumber("propertiesWritten").longValue());
            assertEquals(TEST_GRAPH_SAME_PROPERTIES, row.getString("graphName"));
            assertEquals(List.of("newNodeProp1"), row.get("nodeProperties"));
        });

        var validationQuery =
            "MATCH (n) WHERE n.newNodeProp1 IS NOT NULL RETURN count(n) AS number_of_properties_written";

        assertCypherResult(validationQuery, singletonList(
            Map.of("number_of_properties_written", 6L)
        ));
    }
}
