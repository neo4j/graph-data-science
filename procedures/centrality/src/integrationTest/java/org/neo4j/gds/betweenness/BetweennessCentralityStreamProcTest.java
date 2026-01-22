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
package org.neo4j.gds.betweenness;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

class BetweennessCentralityStreamProcTest extends BaseProcTest {


    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (d:Node {name: 'd'})" +
        ", (e:Node {name: 'e'})" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(d)" +
        ", (d)-[:REL]->(e)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            BetweennessCentralityStreamProc.class,
            GraphProjectProc.class
        );

        runQuery(
            GdsCypher.call(DEFAULT_GRAPH_NAME)
                .graphProject()
                .loadEverything(Orientation.NATURAL)
                .yields()
        );
    }

    @Test
    void testStream() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.betweenness")
            .streamMode()
            .yields();

        var expectedResultMap = Map.of(
            idFunction.of("a"), 0.0,
            idFunction.of("b"), 3.0,
            idFunction.of("c"), 4.0,
            idFunction.of("d"), 3.0,
            idFunction.of("e"), 0.0
        );

        var rowCount = runQueryWithRowConsumer(query, (resultRow) -> {

            var nodeId = resultRow.getNumber("nodeId");
            var expectedScore = expectedResultMap.get(nodeId);

            assertThat(resultRow.getNumber("score")).asInstanceOf(DOUBLE).isCloseTo(
                expectedScore,
                Offset.offset(1e-6)
            );
            
        });
        assertThat(rowCount).isEqualTo(5l);
    }

    // FIXME: This should not be tested here
    @Test
    void shouldValidateSampleSize() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.betweenness")
            .streamMode()
            .addParameter("samplingSize", -42)
            .yields();

        assertError(query, "Configuration parameter 'samplingSize' must be a positive number, got -42.");
    }

    @Test
    void shouldFailOnMixedProjections() {
        runQuery(
            "CALL gds.graph.project(" +
            "   'mixedGraph', " +
            "   '*', " +
            "   {" +
            "       N: {type: 'REL', orientation: 'NATURAL'}, " +
            "       U: {type: 'REL', orientation: 'UNDIRECTED'}" +
            "   }" +
            ")"
        );

        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(() -> runQuery("CALL gds.betweenness.stream('mixedGraph', {})"))
            .withRootCauseInstanceOf(IllegalArgumentException.class)
            .withMessageContaining("Combining UNDIRECTED orientation with NATURAL or REVERSE is not supported.");
    }
}
