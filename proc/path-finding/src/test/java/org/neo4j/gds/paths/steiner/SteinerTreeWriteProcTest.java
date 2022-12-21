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
package org.neo4j.gds.paths.steiner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

class SteinerTreeWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE " +
        " (a:Node) " +
        " ,(b:Node) " +
        " ,(c:Node) " +
        " ,(a)-[:TYPE {cost: 5.4}]->(b) ";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(SteinerTreeWriteProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("TYPE")
            .withRelationshipProperty("cost")
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testYields() {

        var sourceNode = idFunction.of("a");
        var terminalNode = idFunction.of("b");

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.steinerTree")
            .writeMode()
            .addParameter("sourceNode", sourceNode)
            .addParameter("targetNodes", List.of(terminalNode))
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", "STEINER")
            .addParameter("writeProperty", "cost")
            .yields();

        runQuery(query, result -> {
            assertThat(result.columns()).containsExactlyInAnyOrder(
                "preProcessingMillis",
                "computeMillis",
                "effectiveNodeCount",
                "effectiveTargetNodesCount",
                "totalWeight",
                "configuration",
                "writeMillis",
                "relationshipsWritten"
            );
            assertThat(result.hasNext())
                .as("Result should have a single row")
                .isTrue();
            var resultRow = result.next();

            assertThat(resultRow)
                .containsEntry("effectiveNodeCount", 2L)
                .containsEntry("effectiveTargetNodesCount", 1L)
                .containsEntry("totalWeight", 5.4)
                .containsEntry("relationshipsWritten", 1L);

            assertThat(result.hasNext())
                .as("There should be no more result rows")
                .isFalse();

            return false;

        });
    }

    @Test
    void testWrittenRelationships() {

        var sourceNode = idFunction.of("a");
        var terminalNode = idFunction.of("b");

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.steinerTree")
            .writeMode()
            .addParameter("sourceNode", sourceNode)
            .addParameter("targetNodes", List.of(terminalNode))
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", "STEINER")
            .addParameter("writeProperty", "cost")
            .yields();

        runQuery(query);

        var rowCounter = new LongAdder();
        runQueryWithRowConsumer(
            "MATCH (a)-[r:STEINER]->(b) RETURN id(a) AS a, id(b) AS b, r.cost AS cost",
            row -> {
                var a = row.getNumber("a").longValue();
                assertThat(a)
                    .as("The source node should be the same as one specified in the procedure configuration")
                    .isEqualTo(sourceNode);
                var b = row.getNumber("b").longValue();
                assertThat(b)
                    .as("The terminal node should be the same as one specified in the procedure configuration")
                    .isEqualTo(terminalNode);
                var writtenCost = row.getNumber("cost").doubleValue();
                assertThat(writtenCost)
                    .as("The relationship property shoud be written correctly")
                    .isEqualTo(5.4);

                rowCounter.increment();
            }
        );

        assertThat(rowCounter.longValue()).isEqualTo(1L);
    }

}
