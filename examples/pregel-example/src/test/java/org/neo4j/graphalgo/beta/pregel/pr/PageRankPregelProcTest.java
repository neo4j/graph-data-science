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
package org.neo4j.graphalgo.beta.pregel.pr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.neo4j.graphalgo.beta.pregel.pr.PageRankPregel.PAGE_RANK;

class PageRankPregelProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String TEST_GRAPH =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +
        ", (j:Node)" +
        ", (k:Node)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(b)" +
        ", (d)-[:REL]->(a)" +
        ", (d)-[:REL]->(b)" +
        ", (e)-[:REL]->(b)" +
        ", (e)-[:REL]->(d)" +
        ", (e)-[:REL]->(f)" +
        ", (f)-[:REL]->(b)" +
        ", (f)-[:REL]->(e)" +
        ", (g)-[:REL]->(b)" +
        ", (g)-[:REL]->(e)" +
        ", (h)-[:REL]->(b)" +
        ", (h)-[:REL]->(e)" +
        ", (i)-[:REL]->(b)" +
        ", (i)-[:REL]->(e)" +
        ", (j)-[:REL]->(e)" +
        ", (k)-[:REL]->(e)";
    
    private static final double RESULT_ERROR = 1e-3;

    private List<Map<String, Object>> expected;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, PageRankPregelStreamProc.class, PageRankPregelMutateProc.class);

        expected = List.of(
            Map.of("nodeId", idFunction.of("a"), "score", closeTo(0.0276D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("b"), "score", closeTo(0.3483D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("c"), "score", closeTo(0.2650D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("d"), "score", closeTo(0.0330D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("e"), "score", closeTo(0.0682D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("f"), "score", closeTo(0.0330D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("g"), "score", closeTo(0.0136D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("h"), "score", closeTo(0.0136D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("i"), "score", closeTo(0.0136D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("j"), "score", closeTo(0.0136D, RESULT_ERROR)),
            Map.of("nodeId", idFunction.of("k"), "score", closeTo(0.0136D, RESULT_ERROR))
        );
    }

    @Test
    void stream() {
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "pr")
            .streamMode()
            .addParameter("maxIterations", 10)
            .yields("nodeId", "values");

        assertCypherResult(query + " RETURN nodeId, values.pagerank AS score", expected);
    }

    @Test
    void streamSeeded() {
        var createGraphQuery = GdsCypher.call()
            .loadEverything()
            .graphCreate("test")
            .yields();

        runQuery(createGraphQuery);

        var mutateQuery = GdsCypher.call()
            .explicitCreation("test")
            .algo("example", "pregel", "pr")
            .mutateMode()
            .addParameter("maxIterations", 5)
            .addParameter("mutateProperty", "value_")
            .yields();

        runQuery(mutateQuery);

        var query = GdsCypher.call()
            .explicitCreation("test")
            .algo("example", "pregel", "pr")
            .streamMode()
            // we need 11 iterations in total to achieve the same result
            // as the above test since for the computation iteration 6
            // is the initial superstep where it doesn't receive messages
            .addParameter("maxIterations", 6)
            .addParameter("seedProperty", "value_" + PAGE_RANK)
            .yields("nodeId", "values");

        assertCypherResult(query + " RETURN nodeId, values.pagerank AS score", expected);
    }
}
