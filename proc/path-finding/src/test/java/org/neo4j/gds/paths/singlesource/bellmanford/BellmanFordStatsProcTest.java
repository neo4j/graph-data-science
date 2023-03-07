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
package org.neo4j.gds.paths.singlesource.bellmanford;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

class BellmanFordStatsProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String LOOP_DB_CYPHER =
        "CREATE " +
        "  (a0:Node {id: 0})," +
        "  (a1:Node {id: 1})," +
        "  (a2:Node {id: 2})," +
        "  (a3:Node {id: 3})," +
        "  (a4:Node {id: 4})," +
        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a0)-[:R {weight: 10.0}]->(a2), " +
        "  (a2)-[:R {weight: -8.0}]->(a3), " +
        "  (a3)-[:R {weight: -4.0}]->(a4), " +
        "  (a4)-[:R {weight: 1.0}]->(a2) ";
    @Inject
    public IdFunction idFunction;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            BellmanFordStatsProc.class
        );

        var projectQuery="CALL gds.graph.project('graph', '*' , { R : { properties :'weight' }})";
        runQuery(projectQuery);
    }

    @Test
    void shouldReturnCorrectStatsResult() {
        var rowCounter = new LongAdder();
        runQueryWithRowConsumer(
            " MATCH (n) WHERE  n.id = 0 " +
            " CALL gds.bellmanFord.stats('graph', {sourceNode: n, relationshipWeightProperty: 'weight'}) " +
            " YIELD containsNegativeCycle " +
            " RETURN containsNegativeCycle",
            row -> {
                assertThat(row.getBoolean("containsNegativeCycle"))
                    .as("This graph should contain a negative cycle.")
                    .isTrue();

                rowCounter.increment();
            });

        assertThat(rowCounter.longValue()).isEqualTo(1L);
    }


}
