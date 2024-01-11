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
package org.neo4j.gds.beta.pregel.pr;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.neo4j.gds.TestSupport.assertCypherMemoryEstimation;
import static org.neo4j.gds.beta.pregel.pr.PageRankPregel.PAGE_RANK;

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

    @Inject
    private IdFunction idFunction;

    private Map<Long, Double> expected;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, PageRankPregelStreamProc.class, PageRankPregelMutateProc.class);

        expected = Map.ofEntries(
            Map.entry(idFunction.of("a"), 0.0276D),
            Map.entry(idFunction.of("b"), 0.3483D),
            Map.entry(idFunction.of("c"), 0.2650D),
            Map.entry(idFunction.of("d"), 0.0330D),
            Map.entry(idFunction.of("e"), 0.0682D),
            Map.entry(idFunction.of("f"), 0.0330D),
            Map.entry(idFunction.of("g"), 0.0136D),
            Map.entry(idFunction.of("h"), 0.0136D),
            Map.entry(idFunction.of("i"), 0.0136D),
            Map.entry(idFunction.of("j"), 0.0136D),
            Map.entry(idFunction.of("k"), 0.0136D)
        );
    }

    @Test
    void stream() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("example", "pregel", "pr")
            .streamMode()
            .addParameter("maxIterations", 10)
            .yields("nodeId", "values");

        var rowCount = runQueryWithRowConsumer(
            query + " RETURN nodeId as nodeId, values.pagerank AS score",
            resultRow -> {
                var nodeId = resultRow.getNumber("nodeId").longValue();
                var expectedScore = expected.get(nodeId);
                assertThat(resultRow.getNumber("score"))
                    .asInstanceOf(DOUBLE).isCloseTo(expectedScore, Offset.offset(RESULT_ERROR));
            }
        );

        assertThat(rowCount).isEqualTo(expected.size());
    }

    @Test
    void streamEstimate() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("example", "pregel", "pr")
            .streamEstimation()
            .addParameter("maxIterations", 10)
            .yields("bytesMin", "bytesMax", "nodeCount", "relationshipCount");

        assertCypherMemoryEstimation(db, query, MemoryRange.of(768), 11, 17);
    }

    @Test
    void streamSeeded() {
        var createGraphQuery = GdsCypher.call("test")
            .graphProject()
            .loadEverything()
            .yields();

        runQuery(createGraphQuery);

        var mutateQuery = GdsCypher.call("test")
            .algo("example", "pregel", "pr")
            .mutateMode()
            .addParameter("maxIterations", 5)
            .addParameter("mutateProperty", "value_")
            .yields();

        runQuery(mutateQuery);

        var query = GdsCypher.call("test")
            .algo("example", "pregel", "pr")
            .streamMode()
            // we need 11 iterations in total to achieve the same result
            // as the above test since for the computation iteration 6
            // is the initial superstep where it doesn't receive messages
            .addParameter("maxIterations", 6)
            .addParameter("seedProperty", "value_" + PAGE_RANK)
            .yields("nodeId", "values");

        var rowCount = runQueryWithRowConsumer(
            query + " RETURN nodeId, values.pagerank AS score",
            resultRow -> {
                var nodeId = resultRow.getNumber("nodeId").longValue();
                var expectedScore = expected.get(nodeId);
                assertThat(resultRow.getNumber("score"))
                    .asInstanceOf(DOUBLE).isCloseTo(expectedScore, Offset.offset(RESULT_ERROR));
            }
        );

        assertThat(rowCount).isEqualTo(expected.size());
    }
}
