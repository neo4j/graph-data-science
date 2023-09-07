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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.pregel.HitsMutateProc;

import static org.assertj.core.api.Assertions.assertThatNoException;

class HitsMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
            "  (a:Website {name: 'A'})," +
            "  (b:Website {name: 'B'})," +
            "  (c:Website {name: 'C'})," +
            "  (d:Website {name: 'D'})," +
            "  (e:Website {name: 'E'})," +
            "  (f:Website {name: 'F'})," +
            "  (g:Website {name: 'G'})," +
            "  (h:Website {name: 'H'})," +
            "  (i:Website {name: 'I'})," +

            "  (a)-[:LINK]->(b)," +
            "  (a)-[:LINK]->(c)," +
            "  (a)-[:LINK]->(d)," +
            "  (b)-[:LINK]->(c)," +
            "  (b)-[:LINK]->(d)," +
            "  (c)-[:LINK]->(d)," +

            "  (e)-[:LINK]->(b)," +
            "  (e)-[:LINK]->(d)," +
            "  (e)-[:LINK]->(f)," +
            "  (e)-[:LINK]->(h)," +

            "  (f)-[:LINK]->(g)," +
            "  (f)-[:LINK]->(i)," +
            "  (f)-[:LINK]->(h)," +
            "  (g)-[:LINK]->(h)," +
            "  (g)-[:LINK]->(i)," +
            "  (h)-[:LINK]->(i)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            HitsMutateProc.class,
            GraphProjectProc.class
        );

        runQuery(
            "CALL gds.graph.project(" +
            "  'myGraph'," +
            "  'Website'," +
            "  {LINK: {indexInverse: true}}" +
            ");");
    }

    @Test
    void shouldRunWithoutError() {
        assertThatNoException()
            .as("The `HITS` write procedure should run without raising an exception.")
            .isThrownBy(() -> runQuery("CALL gds.alpha.hits.mutate('myGraph', { hitsIterations: 1, writeProperty:'hits' })"));
    }
}
