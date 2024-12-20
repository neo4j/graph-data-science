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
package org.neo4j.gds.sllpa;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.beta.generator.GraphGenerateProc;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG_ARRAY;

class SpeakerListenerLPAWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (a)-[:REL]->(b)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            SpeakerListenerLPAWriteProc.class,
            GraphProjectProc.class,
            GraphGenerateProc.class
        );

        runQuery(
            GdsCypher.call(DEFAULT_GRAPH_NAME)
                .graphProject()
                .loadEverything(Orientation.NATURAL)
                .yields()
        );
    }

    @Test
    void testWrite() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.sllpa")
            .writeMode()
            .addParameter("maxIterations",1)
            .addParameter("writeProperty","foo")
            .yields();


        var rowCount = runQueryWithRowConsumer(query, (resultRow) -> {
            assertThat(resultRow.getNumber("ranIterations")).asInstanceOf(LONG).isEqualTo(1L);
            assertThat(resultRow.getNumber("nodePropertiesWritten")).asInstanceOf(LONG).isEqualTo(3L);

        });
        assertThat(rowCount).isEqualTo(1);
        HashSet<Long> expected =new HashSet<>(List.of(0L,1L,2L));

        var influentialQueryRow = runQueryWithRowConsumer(
            "MATCH (n) RETURN n.foo AS foo",
            resultRow -> {
                assertThat(resultRow.get("foo"))
                    .asInstanceOf(LONG_ARRAY)
                    .satisfies( v ->{
                            assertThat(expected.contains(v[0])).isTrue();
                            expected.remove(v[0]);
                    });
            }
        );

        assertThat(influentialQueryRow).isEqualTo(3);
    }

    @Test
    void shouldFailWhenRunningOnNonWritableGraph() {
        runQuery("CALL gds.graph.generate('randomGraph', 5, 2, {relationshipSeed:19}) YIELD name, nodes, relationships, relationshipDistribution");

        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(
                () -> runQuery("CALL gds.alpha.sllpa.write('randomGraph', {writeProperty: 'm', maxIterations: 4, minAssociationStrength: 0.1})")
            )
            .withRootCauseInstanceOf(IllegalArgumentException.class)
            .withMessageContaining("The provided graph does not support `write` execution mode.");
        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(
                () -> runQuery("CALL gds.sllpa.write('randomGraph', {writeProperty: 'm', maxIterations: 4, minAssociationStrength: 0.1})")
            )
            .withRootCauseInstanceOf(IllegalArgumentException.class)
            .withMessageContaining("The provided graph does not support `write` execution mode.");
    }

}
