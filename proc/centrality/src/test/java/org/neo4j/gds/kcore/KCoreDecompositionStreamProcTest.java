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
package org.neo4j.gds.kcore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;


class KCoreDecompositionStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (z:node)," +
        "  (a:node)," +
        "  (b:node)," +
        "  (c:node)," +
        "  (d:node)," +
        "  (e:node)," +
        "  (f:node)," +
        "  (g:node)," +
        "  (h:node)," +

        "(a)-[:R]->(b)," +
        "(b)-[:R]->(c)," +
        "(c)-[:R]->(d)," +
        "(d)-[:R]->(e)," +
        "(e)-[:R]->(f)," +
        "(f)-[:R]->(g)," +
        "(g)-[:R]->(h)," +
        "(h)-[:R]->(c)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            KCoreDecompositionStreamProc.class,
            GraphProjectProc.class
        );

        runQuery(
            GdsCypher.call("graph")
                .graphProject()
                .withAnyLabel()
                .withRelationshipType("R", Orientation.UNDIRECTED)
                .yields()
        );
    }

    @Test
    void shouldStream(){

        String query="CALL gds.kcore.stream('graph')";

        var expectedOutput= Map.of(
            idFunction.of("z"), 0,
            idFunction.of("a"), 1,
            idFunction.of("b"), 1,
            idFunction.of("c"), 2,
            idFunction.of("d"), 2,
            idFunction.of("e"), 2,
            idFunction.of("f"), 2,
            idFunction.of("g"), 2,
            idFunction.of("h"), 2
        );

        var rowCount = runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            int coreValue = row.getNumber("coreValue").intValue();
            assertThat(expectedOutput).containsEntry(nodeId, coreValue);
        });

        assertThat(rowCount)
            .as("Streamed rows should match the expected")
            .isEqualTo(expectedOutput.size());
    }

    @Test
    void memoryEstimation() {
        String query="CALL gds.kcore.stream.estimate({nodeCount: 100, relationshipCount: 200, nodeProjection: '*', relationshipProjection: '*'}, {})";

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("bytesMin")).asInstanceOf(LONG).isEqualTo(302_184L);
            assertThat(row.getNumber("bytesMax")).asInstanceOf(LONG).isEqualTo(302_184L);
        });

        assertThat(rowCount)
            .as("`estimate` mode should always return one row")
            .isEqualTo(1);

    }

}
