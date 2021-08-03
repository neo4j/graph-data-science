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
package org.neo4j.gds.beta.pregel.lp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.graphalgo.Orientation;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LabelPropagationPregelProcTest extends BaseProcTest {

    private static final String TEST_GRAPH =
        "CREATE" +
        "  (nAlice:User)" +
        ", (nBridget:User)" +
        ", (nCharles:User)" +
        ", (nDoug:User)" +
        ", (nMark:User)" +
        ", (nMichael:User)" +
        ", (nAlice)-[:FOLLOW   {weight: 1.0}]->(nBridget)" +
        ", (nAlice)-[:FOLLOW   {weight: 1.0}]->(nCharles)" +
        ", (nMark)-[:FOLLOW    {weight: 1.0}]->(nDoug)" +
        ", (nBridget)-[:FOLLOW {weight: 1.0}]->(nMichael)" +
        ", (nDoug)-[:FOLLOW    {weight: 2.0}]->(nMark)" +
        ", (nMichael)-[:FOLLOW {weight: 1.0}]->(nAlice)" +
        ", (nAlice)-[:FOLLOW   {weight: 1.0}]->(nMichael)" +
        ", (nBridget)-[:FOLLOW {weight: 1.0}]->(nAlice)" +
        ", (nMichael)-[:FOLLOW {weight: 1.0}]->(nBridget)" +
        ", (nCharles)-[:FOLLOW {weight: 1.0}]->(nDoug)";

    @BeforeEach
    void setup() throws Exception {
        runQuery(TEST_GRAPH);

        registerProcedures(LabelPropagationPregelStreamProc.class);
    }

    @Test
    void stream() {
        var query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("example", "pregel", "lp")
            .streamMode()
            .addParameter("maxIterations", 10)
            .yields("nodeId", "values");

        HashMap<Long, Long> actual = new HashMap<>();
        runQueryWithRowConsumer(query, r -> {
            actual.put(
                r.getNumber("nodeId").longValue(),
                ((Map<String, Long>) r.get("values")).get(LabelPropagationPregel.LABEL_KEY)
            );
        });

        var expected = Map.of(
            0L, 0L,
            1L, 0L,
            2L, 0L,
            3L, 4L,
            4L, 3L,
            5L, 0L
        );

        assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);
    }
}
