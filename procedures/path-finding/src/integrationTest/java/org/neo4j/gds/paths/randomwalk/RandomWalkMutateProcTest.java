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
package org.neo4j.gds.paths.randomwalk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class RandomWalkMutateProcTest extends BaseProcTest {

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
        registerProcedures(RandomWalkMutateProc.class, GraphProjectProc.class);

        runQuery("""
        CALL gds.graph.project(
          'graph',
          ['Node1', 'Node2'],
          { R: {type: '*', orientation: 'UNDIRECTED'}}
        )
        """);
    }

    @Test
    void shouldRun() {
        var rowCount = runQueryWithRowConsumer(
            "CALL gds.randomWalk.mutate('graph', {walksPerNode: 3, walkLength: 10, mutateProperty: 'rw'})",
            row -> {
                assertThat(row.getNumber("preProcessingMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0L);
                assertThat(row.getNumber("mutateMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0L);
                assertThat(row.getNumber("computeMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0L);
                assertThat(row.get("configuration"))
                    .isNotNull()
                    .asInstanceOf(MAP)
                    .containsEntry("walksPerNode", 3)
                    .containsEntry("walkLength", 10)
                    .containsEntry("mutateProperty", "rw");

                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(3L);
            }
        );

        assertThat(rowCount)
            .as("Should have one result row.")
            .isEqualTo(1L);
    }
}
