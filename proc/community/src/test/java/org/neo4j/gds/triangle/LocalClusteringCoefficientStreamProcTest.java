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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalClusteringCoefficientStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
                                           "(a:A { name: 'a', seed: 2 })-[:T]->(b:A { name: 'b', seed: 2 }), " +
                                           "(b)-[:T]->(c:A { name: 'c', seed: 1 }), " +
                                           "(c)-[:T]->(a), " +
                                           "(a)-[:T]->(d:A { name: 'd', seed: 2 }), " +
                                           "(b)-[:T]->(d), " +
                                           "(c)-[:T]->(d), " +
                                           "(a)-[:T]->(e:A { name: 'e', seed: 2 }), " +
                                           "(b)-[:T]->(e) ";

    @Inject
    IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            LocalClusteringCoefficientStreamProc.class
        );
        runQuery(
            "CALL gds.graph.project('graph', {A: {label: 'A', properties: 'seed'}}, {T: {orientation: 'UNDIRECTED'}})");
    }

    @Test
    void testStreaming() {
        var query = "CALL gds.localClusteringCoefficient.stream('graph')";

        var expectedResult = Map.of(
            idFunction.of("a"), 2.0 / 3,
            idFunction.of("b"), 2.0 / 3,
            idFunction.of("c"), 1.0,
            idFunction.of("d"), 1.0,
            idFunction.of("e"), 1.0
        );

        var rowCount = runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double localClusteringCoefficient = row.getNumber("localClusteringCoefficient").doubleValue();
            assertThat(localClusteringCoefficient).isCloseTo(expectedResult.get(nodeId), Offset.offset(1e-5));
        });
        assertThat(rowCount).isEqualTo(5L);

    }

    @Test
    void testStreamingSeeded() {
        var query = "CALL gds.localClusteringCoefficient.stream('graph', { triangleCountProperty: 'seed'})";

        var expectedResult = Map.of(
            idFunction.of("a"), 1.0 / 3,
            idFunction.of("b"), 1.0 / 3,
            idFunction.of("c"), 1.0 / 3,
            idFunction.of("d"), 2.0 / 3,
            idFunction.of("e"), 2.0
        );
        var rowCount = runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double localClusteringCoefficient = row.getNumber("localClusteringCoefficient").doubleValue();
            assertThat(localClusteringCoefficient).isCloseTo(expectedResult.get(nodeId), Offset.offset(1e-5));
        });
        assertThat(rowCount).isEqualTo(5L);
    }

    @Test
    void shouldThrowForNotUndirected() {
        runQuery("CALL gds.graph.project('graph2', 'A', {T: { orientation: 'NATURAL'}})");

        var query = "CALL gds.localClusteringCoefficient.stream('graph2');";
        assertThatThrownBy(() -> runQuery(query)).hasMessageContaining("not all undirected");
    }

}
