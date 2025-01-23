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
package org.neo4j.gds.articulationpoints;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.registerProcedures;

class ArticulationPointsStreamProcTest extends BaseTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        """
            CREATE
               (a1:Node),
               (a2:Node),
               (a3:Node),
               (a4:Node),
               (a5:Node),
               (a6:Node),
               (a7:Node),
               (a8:Node),
               (a9:Node),
               (a10:Node),
               (a11:Node),
               (a12:Node),
               (a13:Node),
               (a14:Node),
               (a15:Node),
               (a16:Node),
               (a1)-[:R]->(a2),
               (a3)-[:R]->(a4),
               (a3)-[:R]->(a7),
               (a7)-[:R]->(a8),
               (a5)-[:R]->(a9),
               (a5)-[:R]->(a10),
               (a9)-[:R]->(a10),
               (a9)-[:R]->(a14),
               (a10)-[:R]->(a11),
               (a11)-[:R]->(a12),
               (a10)-[:R]->(a14),
               (a11)-[:R]->(a15),
               (a12)-[:R]->(a16),
               (a13)-[:R]->(a14),
               (a15)-[:R]->(a16)
            """;

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            db,
            ArticulationPointsStreamProc.class,
            GraphProjectProc.class
        );

        runQuery("CALL gds.graph.project('graph', 'Node', {R: {orientation: 'UNDIRECTED'}})");
    }

    @Test
    void shouldStreamBackResults() {

        var expectedAnswer = Map.of(
            idFunction.of("a3"), List.of(1L, 2L, 2L),
            idFunction.of("a7"), List.of(1L, 2L, 2L),
            idFunction.of("a10"), List.of(4L, 4L, 2L),
            idFunction.of("a11"), List.of(3L, 5L, 2L),
            idFunction.of("a14"), List.of(1L, 7L, 2L)

        );

        var resultRowCount = runQueryWithRowConsumer(
            "CALL gds.articulationPoints.stream('graph')",
            row -> {
                var nodeId = row.getNumber("nodeId");
                assertThat(nodeId)
                    .asInstanceOf(LONG)
                    .isIn(expectedAnswer.keySet());

                var map = row.get("resultingComponents");
               var  expectedList = expectedAnswer.get(nodeId);

                assertThat(map)
                    .asInstanceOf(MAP)
                    .satisfies(
                        objectMap ->{
                            assertThat(objectMap.get("min")).asInstanceOf(LONG).isEqualTo(expectedList.get(0));
                            assertThat(objectMap.get("max")).asInstanceOf(LONG).isEqualTo(expectedList.get(1));
                            assertThat(objectMap.get("count")).asInstanceOf(LONG).isEqualTo(expectedList.get(2));
                        }
                    );
            }
        );

        assertThat(resultRowCount)
            .as("There should be five articulation points.")
            .isEqualTo(5L);
    }

}
