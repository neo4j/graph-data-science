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
package org.neo4j.gds.paths.prizesteiner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PrizeCollectingSteinerTreeWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE(a:Node{p:100.0}) " +
        "CREATE(b:Node{p:100.0}) " +
        "CREATE(c:Node{p:169.0}) " +
        "CREATE (a)-[:TYPE {cost:60.0}]->(b) ";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {

        registerProcedures(PrizeCollectingSteinerTreeWriteProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withNodeProperty("p")
            .withRelationshipType("TYPE", Orientation.UNDIRECTED)
            .withRelationshipProperty("cost")
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testWrite() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.prizeSteinerTree")
            .writeMode()
             .addParameter("prizeProperty","p")
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", "PCST")
            .addParameter("writeProperty", "cost")
            .yields(
                "effectiveNodeCount", "totalWeight", "sumOfPrizes", "relationshipsWritten"
            );

        var rowCount = runQueryWithRowConsumer(query,
        resultRow -> {

            assertThat(resultRow.get("effectiveNodeCount")).isInstanceOf(Long.class);
            assertThat(resultRow.get("totalWeight")).isInstanceOf(Double.class);
            assertThat(resultRow.get("sumOfPrizes")).isInstanceOf(Double.class);
            assertThat(resultRow.get("relationshipsWritten")).isInstanceOf(Long.class);

            assertThat((long) resultRow.get("effectiveNodeCount")).isEqualTo(2L);
            assertThat((double) resultRow.get("totalWeight")).isEqualTo(60);
            assertThat((double) resultRow.get("sumOfPrizes")).isEqualTo(200.0);
            assertThat((long) resultRow.get("relationshipsWritten")).isEqualTo(1L);

        });
        assertThat(rowCount).isEqualTo(1L);

        var sourceNode = idFunction.of("a");
        var terminalNode = idFunction.of("b");

        var rowCountCheck = runQueryWithRowConsumer(
            "MATCH (a)-[r:PCST]->(b) RETURN id(a) AS a, id(b) AS b, r.cost AS cost",
            row -> {
                var a = row.getNumber("a").longValue();
                var b = row.getNumber("b").longValue();

                assertThat(a).isNotEqualTo(b);

                assertThat(a).isIn(List.of(sourceNode,terminalNode));
                assertThat(b).isIn(List.of(sourceNode,terminalNode));

                var writtenCost = row.getNumber("cost").doubleValue();
                assertThat(writtenCost)
                    .isEqualTo(60.0);
            }
        );

        assertThat(rowCountCheck).isEqualTo(1L);

    }
}
