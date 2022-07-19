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
package org.neo4j.gds.paths.traverse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.canonization.CanonicalAdjacencyMatrix.canonicalize;

/**
 * Graph:
 *
 * (b)   (e)
 * 2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 * 1\ 2/ 1\ 2/
 * (c)   (f)
 */
class BfsMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (a)-[:TYPE]->(b)" +
        ", (a)-[:TYPE]->(c)" +
        ", (b)-[:TYPE]->(d)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(e)" +
        ", (d)-[:TYPE]->(f)" +
        ", (e)-[:TYPE]->(g)" +
        ", (f)-[:TYPE]->(g)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(BfsMutateProc.class, GraphProjectProc.class);
    }

    @Test
    void testFindAnyOf() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE")
            .yields();
        runQuery(createQuery);

        long id = idFunction.of("a");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("bfs")
            .mutateMode()
            .addParameter("sourceNode", id)
            .addParameter("targetNodes", List.of(idFunction.of("e"), idFunction.of("f")))
            .addParameter("mutateRelationshipType", "BFS")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 4L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog
            .get(Username.EMPTY_USERNAME.username(), db.databaseName(), DEFAULT_GRAPH_NAME)
            .graphStore()
            .getGraph(RelationshipType.of("BFS"));

        var expected = TestSupport.fromGdl(
            "  (a:Node)-[:BFS]->(b:Node)-[:BFS]->(c:Node)-[:BFS]->(d:Node)-[:BFS]->(e:Node)" +
            ", (f:Node)" +
            ", (g:Node)"
        );

        assertGraphEquals(expected, actual);
    }

    @Test
    void testMaxDepthOut() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE")
            .yields();
        runQuery(createQuery);
        long id = idFunction.of("a");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("bfs")
            .mutateMode()
            .addParameter("sourceNode", id)
            .addParameter("maxDepth", 2)
            .addParameter("mutateRelationshipType", "BFS")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 3L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog
            .get(Username.EMPTY_USERNAME.username(), db.databaseName(), DEFAULT_GRAPH_NAME)
            .graphStore()
            .getGraph(RelationshipType.of("BFS"));

        var expected = TestSupport.fromGdl(
            "  (a:Node)-[:BFS]->(b:Node)-[:BFS]->(c:Node)-[:BFS]->(d:Node)" +
            ", (e:Node)" +
            ", (f:Node)" +
            ", (g:Node)"
        );

        assertGraphEquals(expected, actual);
    }

    @Test
    void testPath() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", Orientation.REVERSE)
            .yields();
        runQuery(createQuery);

        long id = idFunction.of("g");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("bfs")
            .mutateMode()
            .addParameter("sourceNode", id)
            .addParameter("mutateRelationshipType", "BFS")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 6L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog
            .get(Username.EMPTY_USERNAME.username(), db.databaseName(), DEFAULT_GRAPH_NAME)
            .graphStore()
            .getGraph(RelationshipType.of("BFS"));

        // We can't predict the traversal order deterministically => check the possible combinations
        assertThat(canonicalize(actual))
            .isIn(
                canonicalize(TestSupport.fromGdl(
                    "(g:Node)-[:BFS]->(e:Node)-[:BFS]->(f:Node)-[:BFS]->(d:Node)-[:BFS]->(b:Node)-[:BFS]->(c:Node)-[:BFS]->(a:Node)")),
                canonicalize(TestSupport.fromGdl(
                    "(g:Node)-[:BFS]->(e:Node)-[:BFS]->(f:Node)-[:BFS]->(d:Node)-[:BFS]->(c:Node)-[:BFS]->(b:Node)-[:BFS]->(a:Node)")),
                canonicalize(TestSupport.fromGdl(
                    "(g:Node)-[:BFS]->(f:Node)-[:BFS]->(e:Node)-[:BFS]->(d:Node)-[:BFS]->(b:Node)-[:BFS]->(c:Node)-[:BFS]->(a:Node)")),
                canonicalize(TestSupport.fromGdl(
                    "(g:Node)-[:BFS]->(f:Node)-[:BFS]->(e:Node)-[:BFS]->(d:Node)-[:BFS]->(c:Node)-[:BFS]->(b:Node)-[:BFS]->(a:Node)"))
            );
    }
}
