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

import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.beta.generator.GraphGenerateProc;

import static org.assertj.core.api.Assertions.assertThatNoException;

@SuppressWarnings("unchecked")
class RandomWalkGeneratedGraphStreamProcTest extends BaseProcTest {


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(RandomWalkStreamProc.class, GraphGenerateProc.class);
    }

    @Test
    void shouldSkipPathsOnNonDBGraphs() {
            runQuery("CALL gds.graph.generate('graph',10,10, {relationshipSeed:0})");
            assertThatNoException().isThrownBy(() -> {
             var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
                    .algo("gds", "randomWalk")
                    .streamMode()
                    .addParameter("walksPerNode", 3)
                    .addParameter("walkLength", 10)
                    .yields();

                long rowCount = runQueryWithRowConsumer(query, result -> {
                    AssertionsForClassTypes.assertThat(result.get("path")).isNull();
                });
                AssertionsForClassTypes.assertThat(rowCount).isEqualTo(30L);

            });
    }

}
