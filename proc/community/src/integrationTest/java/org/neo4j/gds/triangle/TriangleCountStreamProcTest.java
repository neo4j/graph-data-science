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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class TriangleCountStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
                                           "(a:A)-[:T]->(b:A), " +
                                           "(b)-[:T]->(c:A), " +
                                           "(c)-[:T]->(a)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            TriangleCountStreamProc.class
        );

        runQuery("CALL gds.graph.project('graph', 'A', {T: { orientation: 'UNDIRECTED'}})");
    }

    @Test
    void shouldStream() {

        var query = "CALL gds.triangleCount.stream('graph');";

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("triangleCount"))
                .asInstanceOf(LONG)
                .isEqualTo(1L);
        });

        assertThat(rowCount).isEqualTo(3L);
    }

    @Test
    void shouldThrowForNotUndirected() {
        runQuery("CALL gds.graph.project('graph2', 'A', {T: { orientation: 'NATURAL'}})");
        
        var query = "CALL gds.triangleCount.stream('graph2');";
        assertThatThrownBy(() -> runQuery(query)).hasMessageContaining("not all undirected");
    }

}
