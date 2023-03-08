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
package org.neo4j.gds.beta.node2vec;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecModel;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class Node2VecWriteProcTest extends BaseProcTest implements MemoryEstimateTest<Node2Vec, Node2VecWriteConfig, Node2VecModel.Result> {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node2)" +
        ", (d:Isolated)" +
        ", (e:Isolated)" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(a)" +
        ", (a)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(b)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            Node2VecWriteProc.class,
            GraphProjectProc.class
        );
    }

    @Test
    void embeddingsShouldHaveTheConfiguredDimension() {
        loadGraph(DEFAULT_GRAPH_NAME);
        long dimensions = 42;
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.node2vec")
            .writeMode()
            .addParameter("writeProperty", "embedding")
            .addParameter("embeddingDimension", dimensions)
            .yields();
        runQuery(query);

        assertCypherResult(
            "MATCH (n) RETURN size(n.embedding) AS size",
            List.of(
                Map.of("size", dimensions),
                Map.of("size", dimensions),
                Map.of("size", dimensions),
                Map.of("size", dimensions),
                Map.of("size", dimensions)
            )
        );
    }

    public Class<Node2VecWriteProc> getProcedureClazz() {
        return Node2VecWriteProc.class;
    }

    @Override
    public Node2VecWriteConfig createConfig(CypherMapWrapper userInput) {
        return Node2VecWriteConfig.of(userInput);
    }

    @Test
    void returnLossPerIteration() {
        loadGraph(DEFAULT_GRAPH_NAME);
        int iterations = 5;
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.node2vec")
            .writeMode()
            .addParameter("embeddingDimension", 42)
            .addParameter("writeProperty", "testProp")
            .addParameter("iterations", iterations)
            .yields("lossPerIteration");

        assertCypherResult(query, List.of(Map.of("lossPerIteration", Matchers.hasSize(iterations))));
    }

    @Test
    void shouldThrowIfRunningWouldOverflow() {
        long nodeCount = runQuery("MATCH (n) RETURN count(n) AS count", result ->
            result.<Long>columnAs("count").stream().findFirst().orElse(-1L)
        );
        loadGraph(DEFAULT_GRAPH_NAME);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.node2vec")
            .writeMode()
            .addParameter("writeProperty", "embedding")
            .addParameter("walksPerNode", Integer.MAX_VALUE)
            .addParameter("walkLength", Integer.MAX_VALUE)
            .addParameter("sudo", true)
            .yields();

        Throwable throwable = rootCause(assertThrows(QueryExecutionException.class, () -> runQuery(query)));
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        String expectedMessage = formatWithLocale(
            "Aborting execution, running with the configured parameters is likely to overflow: node count: %d, walks per node: %d, walkLength: %d." +
            " Try reducing these parameters or run on a smaller graph.",
            nodeCount,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE
        );
        assertEquals(expectedMessage, throwable.getMessage());
    }

    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        return userInput.withStringIfMissing("writeProperty", "embedding");
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(Node2VecModel.Result result1, Node2VecModel.Result result2) {
        // TODO: This just tests that the dimensions are the same for node 0, it's not a very good equality test
        assertEquals(result1.embeddings().get(0).data().length, result2.embeddings().get(0).data().length);
    }

}
