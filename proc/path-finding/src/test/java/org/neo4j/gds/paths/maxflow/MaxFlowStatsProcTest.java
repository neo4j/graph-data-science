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
package org.neo4j.gds.paths.maxflow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;

class MaxFlowStatsProcTest extends BaseProcTest {
    @Neo4jGraph(offsetIds = true)
    static final String DB_CYPHER = """
        CREATE
            (a:Node {id: 0}),
            (b:Node {id: 1}),
            (c:Node {id: 2}),
            (d:Node {id: 3}),
            (e:Node {id: 4}),
            (a)-[:R {w: 4.0}]->(d),
            (b)-[:R {w: 3.0}]->(a),
            (c)-[:R {w: 2.0}]->(a),
            (c)-[:R {w: 0.0}]->(b),
            (d)-[:R {w: 5.0}]->(e)
        """;

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(MaxFlowStatsProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withRelationshipProperty("w")
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testYields() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.maxFlow")
            .statsMode()
            .addParameter("sourceNodes", idFunction.of("a"))
            .addParameter("capacityProperty", "w")
            .addParameter("targetNodes", idFunction.of("e"))
            .yields(
                "preProcessingMillis",
                "computeMillis",
                "totalFlow"
            );

        var rowCount = runQueryWithRowConsumer(
            query,
            res -> {
                assertThat(res.getNumber("totalFlow").doubleValue()).isEqualTo(4D);
                assertThat(res.getNumber("preProcessingMillis").longValue()).isGreaterThanOrEqualTo(0L);
                assertThat(res.getNumber("computeMillis").longValue()).isGreaterThanOrEqualTo(0L);
            }
        );
        assertThat(rowCount).isEqualTo(1L);

    }
}
