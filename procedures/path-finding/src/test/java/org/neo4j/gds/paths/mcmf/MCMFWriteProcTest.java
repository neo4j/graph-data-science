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
package org.neo4j.gds.paths.mcmf;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

class MCMFWriteProcTest extends BaseProcTest {
    @Neo4jGraph(offsetIds = true)
    static final String DB_CYPHER = """
            CREATE
                (a:Node {id: 0}),
                (b:Node {id: 1}),
                (c:Node {id: 2}),
                (d:Node {id: 3}),
                (e:Node {id: 4}),
                (a)-[:R {w: 4.0, c:2.0}]->(d),
                (b)-[:R {w: 3.0, c:2.0}]->(a),
                (c)-[:R {w: 2.0, c:2.0}]->(a),
                (c)-[:R {w: 0.0, c:2.0}]->(b),
                (d)-[:R {w: 5.0, c:2.0}]->(e)
            """;

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(MCMFWriteProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withRelationshipProperty("w")
            .withRelationshipProperty("c")

            .yields();
        runQuery(createQuery);
    }

    @Test
    void testYields() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.maxFlow.minCost")
            .writeMode()
            .addParameter("sourceNodes", idFunction.of("a"))
            .addParameter("capacityProperty", "w")
            .addParameter("costProperty", "c")
            .addParameter("targetNodes", idFunction.of("e"))
            .addParameter("writeProperty", "flow")
            .addParameter("writeRelationshipType", "MAX_FLOW")
            .yields(
                "preProcessingMillis",
                "computeMillis",
                "writeMillis",
                "relationshipsWritten",
                "totalFlow",
                "totalCost"
            );

        runQueryWithRowConsumer(
            query,
            res -> {
                assertThat(res.getNumber("preProcessingMillis").longValue()).isGreaterThanOrEqualTo(0L);
                assertThat(res.getNumber("computeMillis").longValue()).isGreaterThanOrEqualTo(0L);
                assertThat(res.getNumber("writeMillis").longValue()).isGreaterThanOrEqualTo(0L);
                assertThat(res.getNumber("relationshipsWritten").longValue()).isEqualTo(2L);
                assertThat(res.getNumber("totalFlow").doubleValue()).isEqualTo(4L);
                assertThat(res.getNumber("totalCost").doubleValue()).isEqualTo(16L);
            }
        );

        var set = new HashSet<Triple<Long, Long, Double>>();
        runQueryWithRowConsumer("MATCH (a)-[r:MAX_FLOW]->(b) return id(a) AS a, id(b) AS b, r.flow AS flow",
            resultRow -> {
                set.add(Triple.of(resultRow.getNumber("a").longValue(), resultRow.getNumber("b").longValue(), resultRow.getNumber("flow").doubleValue()));
            });
        assertThat(set).containsExactlyInAnyOrder(
            Triple.of(idFunction.of("a"), idFunction.of("d"), 4D),
            Triple.of(idFunction.of("d"), idFunction.of("e"), 4D)
        );
        assertThat(set.size()).isEqualTo(2);
    }
}
