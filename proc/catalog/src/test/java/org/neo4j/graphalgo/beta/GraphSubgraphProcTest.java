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
package org.neo4j.graphalgo.beta;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphListProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class GraphSubgraphProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB = "CREATE (a:A)-[:REL]->(b:B)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphSubgraphProc.class, GraphCreateProc.class, GraphListProc.class);

        runQuery(GdsCypher.call()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withAnyRelationshipType()
            .graphCreate("graph")
            .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void executeProc() {
        var subGraphQuery = "CALL gds.beta.graph.subgraph('graph', 'subgraph', {" +
                            "   nodeFilter: 'n:A'," +
                            "   relationshipFilter: 'true'" +
                            "})";

        assertCypherResult(subGraphQuery, List.of(Map.of(
            "configuration", Map.of(
                "nodeFilter", "n:A",
                "relationshipFilter", "true",
                "sudo", false,
                "concurrency", 4
            ),
            "nodeCount", 1L,
            "relationshipCount", 0L,
            "createMillis", greaterThan(0L)
        )));

        var listQuery = "CALL gds.graph.list('subgraph') YIELD nodeFilter, relationshipFilter";

        assertCypherResult(listQuery, List.of(Map.of(
            "nodeFilter", "n:A",
            "relationshipFilter", "true"
        )));

        var subgraphStore = GraphStoreCatalog.get(
            getUsername(),
            db.databaseId(),
            "subgraph"
        ).graphStore();

        assertGraphEquals(fromGdl("(:A)"), subgraphStore.getUnion());
    }
}