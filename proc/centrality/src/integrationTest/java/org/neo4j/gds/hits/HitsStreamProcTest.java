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
package org.neo4j.gds.hits;

import org.assertj.core.data.Offset;
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

 class HitsStreamProcTest  extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (a)-[:REL]->(b)" +
            ", (b)-[:REL]->(c)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            HitsStreamProc.class,
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
            .algo("gds.hits")
            .streamMode()
            .addParameter("hitsIterations",1)
            .yields();

        double authValue =0.7071067811865475;
        double hubValue = 0.7071067811865476;

        var expectedResultMap = Map.of(
            idFunction.of("a"), new double[]{0.0,hubValue},
            idFunction.of("b"),new double[]{authValue,hubValue},
            idFunction.of("c"),new double[]{authValue,0.0}
        );

        var rowCount = runQueryWithRowConsumer(query, (resultRow) -> {

            var nodeId = resultRow.getNumber("nodeId");
            var   auth =(double)((Map<String,Object>)resultRow.get("values")).get("auth");
            var   hub =(double)((Map<String,Object>)resultRow.get("values")).get("hub");
            double[] exp = expectedResultMap.get(nodeId);
            assertThat(auth).isCloseTo(exp[0], Offset.offset(1e-6));
            assertThat(hub).isCloseTo(exp[1], Offset.offset(1e-6));
        });
        assertThat(rowCount).isEqualTo(3l);
    }

}
