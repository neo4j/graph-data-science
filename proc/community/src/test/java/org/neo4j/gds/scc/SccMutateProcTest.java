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
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class SccMutateProcTest extends BaseProcTest {

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
            SccMutateProc.class
        );

        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
    }

    @Test
    void shouldMutate() {

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.scc")
            .mutateMode()
            .addParameter("mutateProperty","scc")
            .yields();

      var rowCount=  runQueryWithRowConsumer(query, row -> {
            long preProcessingMillis = row.getNumber("preProcessingMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();

            assertThat(row.get("componentDistribution"))
                .isInstanceOf(Map.class)
                .asInstanceOf(MAP)
                .containsKeys("p1", "p5", "p10", "p25")
                .containsAllEntriesOf(
                    Map.of(
                        "p99", 3L,
                        "min", 3L,
                        "max", 3L,
                        "mean", 3.0,
                        "p999", 3L,
                        "p95", 3L,
                        "p90", 3L,
                        "p75", 3L,
                        "p50", 3L
                    )
                );

            assertThat(row.getNumber("componentCount").longValue()).isEqualTo(3L);

            assertThat(preProcessingMillis).isNotEqualTo(-1L);
            assertThat(computeMillis).isNotEqualTo(-1L);

        });

      assertThat(rowCount).isEqualTo(1);

      var graph= GraphStoreCatalog
            .get(Username.EMPTY_USERNAME.username(), db.databaseName(), DEFAULT_GRAPH_NAME)
            .graphStore().getUnion();

        var properties=graph.nodeProperties("scc");
        final IntIntScatterMap testMap = new IntIntScatterMap();

        graph.forEachNode( v ->{
            testMap.addTo((int)properties.longValue(v),1);
            return true;
        });

        // 3 sets with 3 elements each
        assertThat(testMap).hasSize(3);
        for (IntIntCursor cursor : testMap) {
            assertThat(cursor.value).isEqualTo(3);
        }


    }

}
