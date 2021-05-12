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
package org.neo4j.graphalgo.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class GraphRemoveNodePropertiesProcTest extends BaseProcTest {

    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";
    private static final String TEST_GRAPH_DIFFERENT_PROPERTIES = "testGraph2";

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
        registerProcedures( GraphCreateProc.class, GraphRemoveNodePropertiesProc.class, FastRPMutateProc.class );
        runQuery(DB_CYPHER);

        runQuery(GdsCypher.call()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withNodeProperty("nodeProp1")
            .withNodeProperty("nodeProp2")
            .withAnyRelationshipType()
            .graphCreate(TEST_GRAPH_SAME_PROPERTIES)
            .yields()
        );

        runQuery(GdsCypher.call()
            .withNodeLabel("A", NodeProjection.of(
                "A",
                PropertyMappings.of().withMappings(
                    PropertyMapping.of("nodeProp1", 1337),
                    PropertyMapping.of("nodeProp2", 1337)
                )
            ))
            .withNodeLabel("B", NodeProjection.of(
                "B",
                PropertyMappings.of().withMappings(
                    PropertyMapping.of("nodeProp1", 1337)
                )
            ))
            .withAnyRelationshipType()
            .graphCreate(TEST_GRAPH_DIFFERENT_PROPERTIES)
            .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void removeNodeProperties() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.removeNodeProperties(" +
            "   '%s', " +
            "   ['nodeProp1', 'nodeProp2']" +
            ") YIELD graphName, nodeProperties, propertiesRemoved",
            TEST_GRAPH_SAME_PROPERTIES
        );

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertEquals(TEST_GRAPH_SAME_PROPERTIES, row.getString("graphName"));
            assertEquals(Arrays.asList("nodeProp1", "nodeProp2"), row.get("nodeProperties"));
            assertEquals(12L, row.getNumber("propertiesRemoved").longValue());
        });
    }

    @Test
    void removeNodePropertiesForLabel() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.removeNodeProperties(" +
            "   '%s', " +
            "   ['nodeProp1', 'nodeProp2'], " +
            "   ['A']" +
            ") YIELD graphName, nodeProperties, propertiesRemoved",
            TEST_GRAPH_SAME_PROPERTIES
        );

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertEquals(TEST_GRAPH_SAME_PROPERTIES, row.getString("graphName"));
            assertEquals(Arrays.asList("nodeProp1", "nodeProp2"), row.get("nodeProperties"));
            assertEquals(6L, row.getNumber("propertiesRemoved").longValue());
        });
    }

    @Test
    void removeNodePropertiesForLabelSubset() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.removeNodeProperties(" +
            "   '%s', " +
            "   ['nodeProp1', 'nodeProp2']" +
            ") YIELD graphName, nodeProperties, propertiesRemoved",
            TEST_GRAPH_DIFFERENT_PROPERTIES
        );

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertEquals(TEST_GRAPH_DIFFERENT_PROPERTIES, row.getString("graphName"));
            assertEquals(Arrays.asList("nodeProp1", "nodeProp2"), row.get("nodeProperties"));
            assertEquals(6L, row.getNumber("propertiesRemoved").longValue());
        });
    }

    @Test
    void shouldFailOnNonExistingNodeProperty() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.removeNodeProperties(" +
                "   '%s', " +
                "   ['nodeProp1', 'nodeProp2', 'nodeProp3']" +
                ")",
                TEST_GRAPH_SAME_PROPERTIES
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(
            rootCause.getMessage(),
            containsString("No node projection with property key(s) ['nodeProp1', 'nodeProp2', 'nodeProp3'] found.")
        );
    }

    @Test
    void shouldReportRemovalOfFastRPProperties() {
        var fastRPCall = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH_SAME_PROPERTIES)
            .algo("fastRP")
            .mutateMode()
            .addParameter("mutateProperty", "fastrp")
            .addParameter("embeddingDimension", 1)
            .yields();

        runQuery(fastRPCall);

        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.removeNodeProperties(" +
            "   '%s', " +
            "   ['nodeProp1', 'nodeProp2', 'fastrp']" +
            ") YIELD graphName, nodeProperties, propertiesRemoved",
            TEST_GRAPH_SAME_PROPERTIES
        );

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertEquals(TEST_GRAPH_SAME_PROPERTIES, row.getString("graphName"));
            assertEquals(Arrays.asList("fastrp", "nodeProp1", "nodeProp2"), row.get("nodeProperties"));
            assertEquals(18L, row.getNumber("propertiesRemoved").longValue());
        });

    }
}
