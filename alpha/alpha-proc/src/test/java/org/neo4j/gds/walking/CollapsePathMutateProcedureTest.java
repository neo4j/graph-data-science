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
package org.neo4j.gds.walking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteRelationshipProc;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.functions.AsNodeFunc;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Data: <a href="https://www.bbc.co.uk/london/travel/downloads/tube_map.html">London Underground map</a>
 *
 * Test illustrates turning paths of segments (colours) into routes, so a "higher level view" of sorts.
 *
 * It is a very customized dataset for a very far fetched use case :)
 */
class CollapsePathMutateProcedureTest extends BaseProcTest {
    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (bakerstreet:Station {name: 'Baker Street'})" +
        ", (barking:Station {name: 'Barking'})" +
        ", (farringdon:Station {name: 'Farringdon'})" +
        ", (kingscross:Station {name: 'Kings Cross St. Pancras'})" +
        ", (liverpoolstreet:Station {name: 'Liverpool Street'})" +
        ", (mileend:Station {name: 'Mile End'})" +
        ", (moorgate:Station {name: 'Moorgate'})" +
        ", (paddington:Station {name: 'Paddington'})" +
        ", (westham:Station {name: 'West Ham'})" +
        ", (whitecity:Station {name: 'White City'})" +
        ", (bakerstreet)-[:BURGUNDY]->(paddington)" +
        ", (barking)-[:GREEN]->(westham)" +
        ", (farringdon)-[:BURGUNDY]->(kingscross)" +
        ", (kingscross)-[:BURGUNDY]->(bakerstreet)" +
        ", (liverpoolstreet)-[:PINK]->(moorgate)" +
        ", (moorgate)-[:YELLOW]->(farringdon)" +
        ", (mileend)-[:PINK]->(liverpoolstreet)" +
        ", (paddington)-[:YELLOW]->(whitecity)" +
        ", (westham)-[:GREEN]->(mileend)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            CollapsePathMutateProc.class,
            GraphProjectProc.class,
            GraphWriteRelationshipProc.class
        );

        registerFunctions(AsNodeFunc.class);

        runQuery("CALL gds.graph.project('cp', '*', ['BURGUNDY', 'GREEN', 'PINK', 'YELLOW'])");
    }

    @Test
    void shouldMutateGraphWithCollapsedPaths() {
        // turn London Underground map into a more high level thing
        String query = "" +
                       "CALL gds.alpha.collapsePath.mutate('cp', { " +
                       "  pathTemplates:" +
                       "    [" +
                       "      ['BURGUNDY', 'BURGUNDY', 'BURGUNDY']," +
                       "      ['GREEN', 'GREEN']," +
                       "      ['PINK', 'PINK']," +
                       "      ['YELLOW']" +
                       "    ], " +
                       "  mutateRelationshipType: 'ROUTE'" +
                       " })";
        Map<String, Object> parameters = Map.of();
        runQueryWithResultConsumer(query, parameters, result -> {
            // establish the shape of output; didn't want to put in it's own test case
            assertThat(result.columns()).containsExactlyInAnyOrder(
                "computeMillis",
                "configuration",
                "mutateMillis",
                "preProcessingMillis",
                "relationshipsWritten"
            );

            assertThat(result.hasNext()).isTrue();
            // we reduced nine segment relationships to five routes
            assertThat((Long) result.next().get("relationshipsWritten")).isEqualTo(5);
            assertThat(result.hasNext()).isFalse(); // there was exactly one output row
        });

        // now to look at the new ROUTE relationships, we need to send them to Neo4j first...
        runQuery("CALL gds.graph.relationship.write('cp', 'ROUTE')");

        // ... and then query and look at end point pairs
        runQueryWithRowConsumer("MATCH (n)-[r:ROUTE]->(m) RETURN n.name AS origin, m.name AS destination", row ->
            assertThat(row).satisfiesAnyOf(
                r -> {
                    assertThat(r.get("origin")).isEqualTo("Barking");
                    assertThat(r.get("destination")).isEqualTo("Mile End");
                },
                r -> {
                    assertThat(r.get("origin")).isEqualTo("Mile End");
                    assertThat(r.get("destination")).isEqualTo("Moorgate");
                },
                r -> {
                    assertThat(r.get("origin")).isEqualTo("Moorgate");
                    assertThat(r.get("destination")).isEqualTo("Farringdon");
                },
                r -> {
                    assertThat(r.get("origin")).isEqualTo("Farringdon");
                    assertThat(r.get("destination")).isEqualTo("Paddington");
                },
                r -> {
                    assertThat(r.get("origin")).isEqualTo("Paddington");
                    assertThat(r.get("destination")).isEqualTo("White City");
                }
            )
        );
    }
}
