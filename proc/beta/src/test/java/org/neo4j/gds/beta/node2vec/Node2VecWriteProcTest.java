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
package org.neo4j.gds.beta.node2vec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class Node2VecWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node2)" +
        ", (d:Isolated)" +
        ", (e:Isolated)" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(a)" +
        ", (a)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(b)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            Node2VecWriteProc.class,
            GraphProjectProc.class
        );

        runQuery("CALL gds.graph.project($graphName, '*', '*')", Map.of("graphName", DEFAULT_GRAPH_NAME));
    }

    @Test
    void embeddingsShouldHaveTheConfiguredDimension() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.node2vec")
            .writeMode()
            .addParameter("writeProperty", "embedding")
            .addParameter("embeddingDimension", 42L)
            .yields();
        runQuery(query);


        var rowCount = runQueryWithRowConsumer("MATCH (n) RETURN size(n.embedding) AS size", row -> {
           assertThat(row.getNumber("size")).asInstanceOf(LONG).isEqualTo(42L);
        });
        assertThat(rowCount).isEqualTo(5);
    }

    @Test
    void returnLossPerIteration() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.node2vec")
            .writeMode()
            .addParameter("embeddingDimension", 42)
            .addParameter("writeProperty", "testProp")
            .addParameter("iterations", 5)
            .yields("lossPerIteration");

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("lossPerIteration")).asList().hasSize(5);
        });

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);
    }

    @Test
    void shouldThrowIfRunningWouldOverflow() {
        long nodeCount = runQuery("MATCH (n) RETURN count(n) AS count", result ->
            result.<Long>columnAs("count").stream().findFirst().orElse(-1L)
        );
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.node2vec")
            .writeMode()
            .addParameter("writeProperty", "embedding")
            .addParameter("walksPerNode", Integer.MAX_VALUE)
            .addParameter("walkLength", Integer.MAX_VALUE)
            .addParameter("sudo", true)
            .yields();

        String expectedMessage = formatWithLocale(
            "Aborting execution, running with the configured parameters is likely to overflow: node count: %d, walks per node: %d, walkLength: %d." +
            " Try reducing these parameters or run on a smaller graph.",
            nodeCount,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE
        );

        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(expectedMessage);
    }
}
