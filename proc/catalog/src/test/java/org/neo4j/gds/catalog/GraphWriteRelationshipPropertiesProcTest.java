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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class GraphWriteRelationshipPropertiesProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (p:Person), " +
        "  (p1:Person), " +
        "  (p2:Person), " +
        "  (p3:Person), " +
        "  (p)-[:PAYS { amount: 1.0}]->(p1), " + //
        "  (p)-[:PAYS { amount: 2.0}]->(p1), " + //
        "  (p)-[:PAYS { amount: 3.0}]->(p1), " + // These become all one relationship in the in-memory graph
        "  (p)-[:PAYS { amount: 4.0}]->(p1), " + //
        "  (p)-[:PAYS { amount: 5.0}]->(p1), " + //
        "  (p)-[:PAYS { amount: 6.0}]->(p1), " + //

        "  (p2)-[:PAYS { amount: 3.0}]->(p1), " +
        "  (p2)-[:PAYS { amount: 4.0}]->(p1), " +
        "  (p3)-[:PAYS { amount: 5.0}]->(p2), " +
        "  (p3)-[:PAYS { amount: 6.0}]->(p2)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, GraphWriteRelationshipPropertiesProc.class);

        runQuery("CALL gds.graph.project( " +
                 "  'aggregate', " +
                 "  ['Person'], " +
                 "  { " +
                 "    PAID: { " +
                 "      type: 'PAYS', " +
                 "      properties: { " +
                 "        totalAmount: { property: 'amount', aggregation: 'SUM' }, " +
                 "        numberOfPayments: { property: 'amount', aggregation: 'COUNT' } " +
                 "      } " +
                 "    } " +
                 "  } " +
                 ")"
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldWriteRelationshipWithMultipleProperties() {
        String writeQuery =
            "CALL gds.graph.relationshipProperties.write('aggregate', 'PAID',[ 'totalAmount', 'numberOfPayments'])" +
            "YIELD writeMillis, graphName, relationshipType, relationshipProperties, relationshipsWritten, propertiesWritten";

        var rowCount = runQueryWithRowConsumer(writeQuery, row -> {
            assertThat(row.getNumber("writeMillis")).asInstanceOf(LONG).isGreaterThanOrEqualTo(0L);
            assertThat(row.getString("graphName")).isEqualTo("aggregate");
            assertThat(row.getString("relationshipType")).isEqualTo("PAID");
            assertThat(row.get("relationshipProperties"))
                .asList()
                .containsExactlyInAnyOrder("totalAmount", "numberOfPayments");

            assertThat(row.getNumber("relationshipsWritten")).asInstanceOf(LONG).isEqualTo(3L);
            assertThat(row.getNumber("propertiesWritten")).asInstanceOf(LONG).isGreaterThanOrEqualTo(6L);
        });

        assertThat(rowCount)
            .as("`write` mode should always return one row")
            .isEqualTo(1);

        assertCypherResult(
            "MATCH ()-[r:PAID]->() RETURN r.totalAmount AS totalAmount, r.numberOfPayments AS numberOfPayments",
            List.of(
                Map.of("totalAmount", 21d, "numberOfPayments", 6d),
                Map.of("totalAmount", 7d, "numberOfPayments", 2d),
                Map.of("totalAmount", 11d, "numberOfPayments", 2d)
            )
        );
    }

}
