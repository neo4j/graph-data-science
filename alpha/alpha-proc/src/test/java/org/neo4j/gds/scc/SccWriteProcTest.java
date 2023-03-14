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
package org.neo4j.gds.scc;

import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SccWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (d:Node {name: 'd'})" +
        ", (e:Node {name: 'e'})" +
        ", (f:Node {name: 'f'})" +
        ", (g:Node {name: 'g'})" +
        ", (h:Node {name: 'h'})" +
        ", (i:Node {name: 'i'})" +

        ", (a)-[:TYPE {cost: 5}]->(b)" +
        ", (b)-[:TYPE {cost: 5}]->(c)" +
        ", (c)-[:TYPE {cost: 5}]->(a)" +

        ", (d)-[:TYPE {cost: 2}]->(e)" +
        ", (e)-[:TYPE {cost: 2}]->(f)" +
        ", (f)-[:TYPE {cost: 2}]->(d)" +

        ", (a)-[:TYPE {cost: 2}]->(d)" +

        ", (g)-[:TYPE {cost: 3}]->(h)" +
        ", (h)-[:TYPE {cost: 3}]->(i)" +
        ", (i)-[:TYPE {cost: 3}]->(g)";
    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            SccWriteProc.class
        );

        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME).graphProject().loadEverything(Orientation.NATURAL).yields();
        runQuery(projectQuery);
    }

    @Test
    void shouldWrite() {

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.alpha.scc")
            .writeMode()
            .addParameter("writeProperty","scc")
            .yields();

        runQueryWithRowConsumer(query, row -> {
            long preProcessingMillis = row.getNumber("preProcessingMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();

            assertThat(row.getNumber("setCount").longValue()).isEqualTo(3L);
            assertThat(row.getNumber("minSetSize").longValue()).isEqualTo(3L);
            assertThat(row.getNumber("maxSetSize").longValue()).isEqualTo(3L);

            assertThat(row.getString("writeProperty")).isEqualTo("scc");
            assertThat(preProcessingMillis).isNotEqualTo(-1L);
            assertThat(computeMillis).isNotEqualTo(-1L);
            assertThat(writeMillis).isNotEqualTo(-1L);

        });

        String validationQuery = "MATCH (n) RETURN n.scc as c";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        runQueryWithRowConsumer(validationQuery, row -> testMap.addTo(row.getNumber("c").intValue(), 1));

        // 3 sets with 3 elements each
        assertEquals(3, testMap.size());
        for (IntIntCursor cursor : testMap) {
            assertEquals(3, cursor.value);
        }
    }

}
