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
 *     (b)   (e)
 *   2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 *   1\ 2/ 1\ 2/
 *    (c)   (f)
 */
class DfsMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
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
        registerProcedures(DfsMutateProc.class, GraphProjectProc.class);
    }

    @Test
    void testMaxDepthOut() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE")
            .yields();
        runQuery(createQuery);

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("dfs")
            .mutateMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("maxDepth", 2)
            .addParameter("mutateRelationshipType", "DFS")
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
            .getGraph(RelationshipType.of("DFS"));

        var expected = TestSupport.fromGdl(
            "  (a:Node)-[:DFS]->(b:Node)-[:DFS]->(c:Node)-[:DFS]->(d:Node)" +
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
            .algo("dfs")
            .mutateMode()
            .addParameter("sourceNode", id)
            .addParameter("mutateRelationshipType", "DFS")
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
            .getGraph(RelationshipType.of("DFS"));

        // We can't predict the traversal order deterministically => check the possible combinations
        assertThat(canonicalize(actual))
            .isIn(
                canonicalize(TestSupport.fromGdl(
                    "(g:Node)-[:DFS]->(e:Node)-[:DFS]->(d:Node)-[:DFS]->(b:Node)-[:DFS]->(a:Node)-[:DFS]->(c:Node)-[:DFS]->(f:Node)")),
                canonicalize(TestSupport.fromGdl(
                    "(g:Node)-[:DFS]->(e:Node)-[:DFS]->(d:Node)-[:DFS]->(c:Node)-[:DFS]->(a:Node)-[:DFS]->(b:Node)-[:DFS]->(f:Node)")),
                canonicalize(TestSupport.fromGdl(
                    "(g:Node)-[:DFS]->(f:Node)-[:DFS]->(d:Node)-[:DFS]->(b:Node)-[:DFS]->(a:Node)-[:DFS]->(c:Node)-[:DFS]->(e:Node)")),
                canonicalize(TestSupport.fromGdl(
                    "(g:Node)-[:DFS]->(f:Node)-[:DFS]->(d:Node)-[:DFS]->(c:Node)-[:DFS]->(a:Node)-[:DFS]->(b:Node)-[:DFS]->(e:Node)"))
            );
    }


}
