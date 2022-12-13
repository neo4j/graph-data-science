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
package org.neo4j.gds.beta.undirected;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ToUndirectedProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB = "CREATE " +
                                    "(a:A) " +
                                    ",(b:B) " +
                                    ",(c:C) " +
                                    ",(a)-[:REL]->(b)" +
                                    ",(a)-[:REL]->(c)" +
                                    ",(b)-[:REL]->(c)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(ToUndirectedProc.class, GraphProjectProc.class);

        runQuery(GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withNodeLabel("C")
            .withRelationshipType("REL")
            .yields()
        );
    }

    @Test
    void convertToUndirected() {
        String query = "CALL gds.beta.graph.relationships.toUndirected('graph', {relationshipType: 'REL', mutateRelationshipType: 'REL2'})";

        assertCypherResult(query, List.of(Map.of("inputRelationships", 3L,
            "relationshipsWritten", 6L,
            "mutateMillis", instanceOf(Long.class),
            "preProcessingMillis",instanceOf(Long.class),
            "computeMillis",instanceOf(Long.class),
            "postProcessingMillis",instanceOf(Long.class),
            "configuration", instanceOf(Map.class))
        ));

        var gs = GraphStoreCatalog.get("", "neo4j", "graph");
        var graph = gs.graphStore().getGraph(RelationshipType.of("REL2"));
        assertGraphEquals(fromGdl(DB.replace("REL", "REL2"), Orientation.UNDIRECTED), graph);
    }

    @Test
    void shouldFailIfMutateRelationshipTypeExists() {
        String query = "CALL gds.beta.graph.relationships.toUndirected('graph', {relationshipType: 'REL', mutateRelationshipType: 'REL'})";

        Throwable throwable = rootCause(assertThrows(QueryExecutionException.class, () -> runQuery(query)));
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        String expectedMessage = formatWithLocale(
            "Relationship type `REL` already exists in the in-memory graph."
        );
        assertEquals(expectedMessage, throwable.getMessage());
    }

    @Test
    void shouldFailIfRelationshipTypeDoesNotExists() {
        String query = "CALL gds.beta.graph.relationships.toUndirected('graph', {relationshipType: 'REL2', mutateRelationshipType: 'REL2'})";

        Throwable throwable = rootCause(assertThrows(QueryExecutionException.class, () -> runQuery(query)));
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        String expectedMessage = formatWithLocale(
            "Could not find the specified `relationshipType` of ['REL2']. Available relationship types are ['REL']."
        );
        assertEquals(expectedMessage, throwable.getMessage());
    }
}