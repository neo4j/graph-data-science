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
package gds.training;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.compat.MapUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

final class K1ColoringProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
            "CREATE" +
                    " (a)" +
                    ",(c)" +
                    ",(b)" +
                    ",(d)" +
                    ",(a)-[:REL]->(b)" +
                    ",(b)-[:REL]->(d)" +
                    ",(a)-[:REL]->(d)" +
                    ",(d)-[:REL]->(c)" +
                    ",(a)-[:REL]->(c)";

    // a = 0
    // b = 1
    // c = 1
    // d = 2

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(K1ColoringProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @Test
    void runK1Coloring() {
        List<Map<String, Object>> expected = Arrays.asList(
                MapUtil.map(
                        "nodeId", 0L,
                        "color", 0L
                ),
                MapUtil.map(
                        "nodeId", 1L,
                        "color", 1L
                ),
                MapUtil.map(
                        "nodeId", 2L,
                        "color", 1L
                ),
                MapUtil.map(
                        "nodeId", 3L,
                        "color", 2L
                )

        );

        String query = "CALL gds.beta.k1coloring.pregel({" +
                "  nodeProjection: '*'," +
                "  relationshipProjection: '*'," +
                "  maxIterations: 5 " +
                "}) YIELD nodeId, color";

        assertCypherResult(query, expected);
    }
}
