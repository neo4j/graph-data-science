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
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.Collections;
import java.util.Map;

class GraphDropRelationshipProcTest extends BaseProcTest {
    private final String DB_CYPHER =
        "CREATE (:A)-[:T1]->(:A), (:A)-[:T2 {p: 1}]->(:A) ";

    private final String graphName = "g";
    private final Map<String, Object> params = Map.of("graphName", this.graphName);

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphDropRelationshipProc.class, GraphProjectProc.class);
        runQuery(DB_CYPHER);
        runQuery("CALL gds.graph.project($graphName, 'A', ['T1', { T2: { properties: 'p'}}])", params);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldDeleteRelationshipType() {
        String query = "CALL gds.graph.relationships.drop('g', 'T1')";
        assertCypherResult(query, Collections.singletonList(Map.of(
                "graphName", "g",
                "relationshipType", "T1",
                "deletedRelationships", 1L,
                "deletedProperties", Map.of()
            )
        ));
    }

    @Test
    void shouldDeleteRelationshipTypeWithProperties() {
        String query = "CALL gds.graph.relationships.drop('g', 'T2')";
        assertCypherResult(query, Collections.singletonList(Map.of(
                "graphName", "g",
                "relationshipType", "T2",
                "deletedRelationships", 1L,
                "deletedProperties", Map.of("p", 1L)
            )
        ));
    }
}
