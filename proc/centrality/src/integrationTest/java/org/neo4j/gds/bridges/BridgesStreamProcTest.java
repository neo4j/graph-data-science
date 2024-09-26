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
package org.neo4j.gds.bridges;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;

class BridgesStreamProcTest  extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (a)-[:REL]->(b)" +
            ", (b)-[:REL]->(c)" +
            ", (c)-[:REL]->(d)" +
            ", (d)-[:REL]->(b)";


    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            BridgesStreamProc.class,
            GraphProjectProc.class
        );

        runQuery(
            GdsCypher.call(DEFAULT_GRAPH_NAME)
                .graphProject()
                .loadEverything(Orientation.UNDIRECTED)
                .yields()
        );
    }

    @Test
    void shouldStreamBackResults(){
            var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
                .algo("gds.bridges")
                .streamMode()
                .yields();

            var expectedNode1 = idFunction.of("a");
            var expectedNode2 = idFunction.of("b");

        var rowCount = runQueryWithRowConsumer(query, (resultRow) -> {
            var fromId = resultRow.getNumber("from");
            var toId = resultRow.getNumber("to");
            assertThat(fromId.longValue()).isNotEqualTo(toId.longValue());
            assertThat(fromId.longValue()).isIn(expectedNode1, expectedNode2);
            assertThat(toId.longValue()).isIn(expectedNode1, expectedNode2);

        });
            assertThat(rowCount).isEqualTo(1l);
        }

}
