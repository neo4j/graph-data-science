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
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.util.Collections;
import java.util.Map;

import static org.neo4j.gds.compat.MapUtil.map;

class GraphDeleteRelationshipProcTest extends BaseProcTest {

    private final String DB_CYPHER =
        "CREATE (:A)-[:T1]->(:A), " +
        "       (:A)-[:T2 {p: 1}]->(:A) ";

    private final String graphName = "g";
    private final Map<String, Object> params = map("graphName", this.graphName);

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphDeleteRelationshipProc.class, GraphCreateProc.class);
        runQuery(DB_CYPHER);
        runQuery("CALL gds.graph.create($graphName, 'A', ['T1', { T2: { properties: 'p'}}])", params);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void failWhenNoSuchGraph() {
        assertError(
            "CALL gds.graph.deleteRelationships('foo', 'bar')",
            "Graph with name `foo` does not exist on database `neo4j`."
        );
    }

    @Test
    void failWhenNoSuchRelType() {
        assertError("CALL gds.graph.deleteRelationships('g', 'bar')", "No relationship type 'bar' found in graph 'g'.");
    }

    @Test
    void failWhenDeletingLastRelType() {
        // deleting one is fine
        runQuery("CALL gds.graph.deleteRelationships('g', 'T1')");

        String query2 = "CALL gds.graph.deleteRelationships('g', 'T2')";

        assertError(
            query2,
            "Deleting the last relationship type ('T2') from a graph ('g') is not supported. " +
            "Use `gds.graph.drop()` to drop the entire graph instead."
        );
    }

    @Test
    void shouldDeleteRelationshipType() {
        String query = "CALL gds.graph.deleteRelationships('g', 'T1')";
        assertCypherResult(query, Collections.singletonList(map(
            "graphName", "g",
            "relationshipType", "T1",
            "deletedRelationships", 1L,
            "deletedProperties", map())
        ));
    }

    @Test
    void shouldDeleteRelationshipTypeWithProperties() {
        String query = "CALL gds.graph.deleteRelationships('g', 'T2')";
        assertCypherResult(query, Collections.singletonList(map(
            "graphName", "g",
            "relationshipType", "T2",
            "deletedRelationships", 1L,
            "deletedProperties", map("p", 1L))
        ));
    }

}
