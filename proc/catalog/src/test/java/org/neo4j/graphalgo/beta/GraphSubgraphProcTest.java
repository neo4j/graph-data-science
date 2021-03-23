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
import org.neo4j.graphalgo.beta.filter.expression.SemanticErrors;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphListProc;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
        var subGraphQuery = "CALL gds.beta.graph.create.subgraph('subgraph', 'graph', 'n:A', 'true')";

        assertCypherResult(subGraphQuery, List.of(Map.of(
            "configuration", Map.of(
                "sudo", false,
                "concurrency", 4
            ),
            "graphName", "subgraph",
            "fromGraphName", "graph",
            "nodeFilter", "n:A",
            "relationshipFilter", "true",
            "nodeCount", 1L,
            "relationshipCount", 0L,
            "createMillis", greaterThan(0L)
        )));

        // Verify that we get the correct output for gds.graph.list().
        // Projections are taken from the original graph, the graph
        // name is the name of the new subgraph and the filters expressions
        // are added.

        var originalGraphCreateConfig = (GraphCreateFromStoreConfig) GraphStoreCatalog
            .get(getUsername(), db.databaseId(), "graph")
            .config();
        var expectedNodeProjection = originalGraphCreateConfig.nodeProjections().toObject();
        var expectedRelationshipProjection = originalGraphCreateConfig.relationshipProjections().toObject();
        var originalCreationTime = originalGraphCreateConfig.creationTime();

        var listQuery = "CALL gds.graph.list('subgraph') " +
                        "YIELD graphName, nodeCount, relationshipCount, creationTime, nodeFilter, relationshipFilter, nodeProjection, relationshipProjection";

        assertCypherResult(listQuery, List.of(Map.of(
            "graphName", "subgraph",
            "nodeCount", 1L,
            "relationshipCount", 0L,
            "creationTime", greaterThan(originalCreationTime),
            "nodeProjection", expectedNodeProjection,
            "relationshipProjection", expectedRelationshipProjection,
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

    @Test
    void throwsOnExistingGraph() {
        runQuery(GdsCypher.call()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withAnyRelationshipType()
            .graphCreate("subgraph")
            .yields());

        var subGraphQuery = "CALL gds.beta.graph.create.subgraph('subgraph', 'graph', 'n:A', 'true')";

        assertThatThrownBy(() -> runQuery(subGraphQuery))
            .getRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("A graph with name 'subgraph' already exists.");
    }

    @Test
    void throwsOnParserError() {
        var subGraphQuery = "CALL gds.beta.graph.create.subgraph('subgraph', 'graph', 'GIMME NODES, JOANNA, GIMME NODES', 'true')";

        assertThatThrownBy(() -> runQuery(subGraphQuery))
            .getRootCause()
            .isInstanceOf(ParseException.class)
            .hasMessageContaining("GIMME NODES, JOANNA, GIMME NODES");
    }

    @Test
    void throwsOnSemanticNodeError() {
        var subGraphQuery = "CALL gds.beta.graph.create.subgraph('subgraph', 'graph', 'r:Foo', 'true')";

        assertThatThrownBy(() -> runQuery(subGraphQuery))
            .getRootCause()
            .isInstanceOf(SemanticErrors.class)
            .hasMessageContaining("Only `n` is allowed for nodes")
            .hasMessageContaining("Unknown label `Foo`");
    }

    @Test
    void throwsOnSemanticRelationshipError() {
        var subGraphQuery = "CALL gds.beta.graph.create.subgraph('subgraph', 'graph', 'true', 'r:BAR AND r.weight > 42')";

        assertThatThrownBy(() -> runQuery(subGraphQuery))
            .getRootCause()
            .isInstanceOf(SemanticErrors.class)
            .hasMessageContaining("Unknown property `weight`.")
            .hasMessageContaining("Unknown relationship type `BAR`.");
    }
}
