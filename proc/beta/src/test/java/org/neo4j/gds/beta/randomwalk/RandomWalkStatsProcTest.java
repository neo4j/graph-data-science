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
package org.neo4j.gds.beta.randomwalk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class RandomWalkStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node2)" +
        ", (d:Isolated)" +
        ", (e:Isolated)" +
        ", (a)-[:REL1]->(b)" +
        ", (b)-[:REL1]->(a)" +
        ", (a)-[:REL1]->(c)" +
        ", (c)-[:REL2]->(a)" +
        ", (b)-[:REL2]->(c)" +
        ", (c)-[:REL2]->(b)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(RandomWalkStatsProc.class, GraphProjectProc.class);

        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabels("Node1", "Node2")
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);
    }

    @Test
    void shouldRun() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "randomWalk")
            .statsMode()
            .addParameter("walksPerNode", 3)
            .addParameter("walkLength", 10)
            .yields();

        var rowCount = new LongAdder();
        runQueryWithRowConsumer(query, row -> {
            var preProcessingMillis = row.getNumber("preProcessingMillis").longValue();
            var computeMillis = row.getNumber("computeMillis").longValue();
            var configuration = row.get("configuration");
            assertThat(preProcessingMillis).isGreaterThanOrEqualTo(0L);
            assertThat(computeMillis).isGreaterThanOrEqualTo(0L);
            assertThat(configuration)
                .isNotNull()
                .asInstanceOf(MAP)
                .containsEntry("walksPerNode", 3)
                .containsEntry("walkLength", 10);

            rowCount.increment();
        });

        assertThat(rowCount.longValue())
            .as("Should have one result row.")
            .isEqualTo(1L);
    }

    @Test
    void shouldRunMemoryEstimation() {
        String query = "CALL gds.randomWalk.stats.estimate($graphName, {})";

        runQuery(
            query,
            Map.of("graphName", DEFAULT_GRAPH_NAME),
            result -> {
                var rowCount = new LongAdder();
                assertThat(result.columns()).containsExactlyInAnyOrder(
                    "requiredMemory",
                    "treeView",
                    "mapView",
                    "bytesMin",
                    "bytesMax",
                    "nodeCount",
                    "relationshipCount",
                    "heapPercentageMin",
                    "heapPercentageMax"
                );

                while (result.hasNext()) {
                    rowCount.increment();
                    result.next();
                }

                assertThat(rowCount.longValue()).isEqualTo(1L);
                return true;
            });
    }
}
