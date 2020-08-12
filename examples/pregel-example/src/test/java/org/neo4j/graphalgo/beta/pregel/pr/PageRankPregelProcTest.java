/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.catalog.GraphCreateProc;

import java.util.HashMap;
import java.util.Map;

class PageRankPregelProcTest extends BaseProcTest {

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

    private static final Map<Long, Double> EXPECTED_RANKS = new HashMap<>();

    static {
        EXPECTED_RANKS.put(0L, 0.0276D);
        EXPECTED_RANKS.put(1L, 0.3483D);
        EXPECTED_RANKS.put(2L, 0.2650D);
        EXPECTED_RANKS.put(3L, 0.0330D);
        EXPECTED_RANKS.put(4L, 0.0682D);
        EXPECTED_RANKS.put(5L, 0.0330D);
        EXPECTED_RANKS.put(6L, 0.0136D);
        EXPECTED_RANKS.put(7L, 0.0136D);
        EXPECTED_RANKS.put(8L, 0.0136D);
        EXPECTED_RANKS.put(9L, 0.0136D);
        EXPECTED_RANKS.put(10L, 0.0136D);
    }

    @BeforeEach
    void setup() throws Exception {
        runQuery(TEST_GRAPH);

        registerProcedures(GraphCreateProc.class, PageRankPregelStreamProc.class, PageRankPregelMutateProc.class);
    }

    @Test
    void stream() {
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "pr")
            .streamMode()
            .addParameter("maxIterations", 10)
            .yields("nodeId", "values");

        HashMap<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query, r -> {
            actual.put(
                r.getNumber("nodeId").longValue(),
                ((Map<String, Double>) r.get("values")).get(Pregel.DEFAULT_NODE_VALUE_KEY)
            );
        });

        assertMapEqualsWithTolerance(EXPECTED_RANKS, actual, 0.001);
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
            .addParameter("mutateProperty", "pageRank")
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
            .addParameter("seedProperty", "pageRank")
            .yields("nodeId", "values");

        HashMap<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query, r -> {
            actual.put(
                r.getNumber("nodeId").longValue(),
                ((Map<String, Double>) r.get("values")).get(Pregel.DEFAULT_NODE_VALUE_KEY)
            );
        });

        assertMapEqualsWithTolerance(EXPECTED_RANKS, actual, 0.001);
    }
}
