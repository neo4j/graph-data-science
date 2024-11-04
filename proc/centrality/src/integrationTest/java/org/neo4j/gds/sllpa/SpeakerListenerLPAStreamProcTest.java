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
package org.neo4j.gds.sllpa;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SpeakerListenerLPAStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (a)-[:REL]->(b)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            SpeakerListenerLPAStreamProc.class,
            GraphProjectProc.class
        );

        runQuery(
            GdsCypher.call(DEFAULT_GRAPH_NAME)
                .graphProject()
                .loadEverything(Orientation.NATURAL)
                .yields()
        );
    }

    @Test
    void testStream() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.sllpa")
            .streamMode()
            .addParameter("maxIterations",1)
            .yields();

        var expectedResultMap = Map.of(
            idFunction.of("a"), new long[]{0},
            idFunction.of("b"),new long[]{1},
            idFunction.of("c"),new long[]{2}
        );

        var rowCount = runQueryWithRowConsumer(query, (resultRow) -> {

            var nodeId = resultRow.getNumber("nodeId");
            var   communities =(long[])((Map<String,Object>)resultRow.get("values")).get("communityIds");
           var expected =  expectedResultMap.get(nodeId);
           assertThat(communities).containsExactlyInAnyOrder(expected);
            
        });
        assertThat(rowCount).isEqualTo(3l);
    }

}
