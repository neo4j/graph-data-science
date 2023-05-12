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
package org.neo4j.gds.embeddings.node2vec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

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
            .addParameter("iterations", 5)
            .yields("lossPerIteration");

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("lossPerIteration"))
                .as("There should be the same amount of losses as the configured `iterations`.")
                .asList()
                .hasSize(5);
        });

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);

        var nodePropertiesWritten = runQueryWithRowConsumer("MATCH (n) RETURN size(n.embedding) AS size", row -> {
           assertThat(row.getNumber("size")).asInstanceOf(LONG).isEqualTo(42L);
        });
        assertThat(nodePropertiesWritten).isEqualTo(5);
    }
}
