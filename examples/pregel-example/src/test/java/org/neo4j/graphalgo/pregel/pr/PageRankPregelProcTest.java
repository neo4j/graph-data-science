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
package org.neo4j.graphalgo.pregel.pr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;

import java.util.HashMap;

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

    @BeforeEach
    void setup() throws Exception {
        runQuery(TEST_GRAPH);

        registerProcedures(PageRankPregelProc.class);
    }

    @Test
    void stream() {
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "pr")
            .streamMode()
            .addParameter("maxIterations", 10)
            .yields("nodeId", "value");

        HashMap<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query, r -> {
            actual.put(r.getNumber("nodeId").longValue(), r.getNumber("value").doubleValue());
        });

        var expected = new HashMap<Long, Double>();
        expected.put(0L, 0.0276D);
        expected.put(1L, 0.3483D);
        expected.put(2L, 0.2650D);
        expected.put(3L, 0.0330D);
        expected.put(4L, 0.0682D);
        expected.put(5L, 0.0330D);
        expected.put(6L, 0.0136D);
        expected.put(7L, 0.0136D);
        expected.put(8L, 0.0136D);
        expected.put(9L, 0.0136D);
        expected.put(10L, 0.0136D);

        assertMapEqualsWithTolerance(expected, actual, 0.001);
    }


}
