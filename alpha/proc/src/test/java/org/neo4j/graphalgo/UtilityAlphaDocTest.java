/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UtilityAlphaDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        registerProcedures(GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class, IsFiniteFunc.class, AsPathFunc.class);

        String dbQuery =
            "CREATE (nAlice:User {name: 'Alice'}) " +
            "CREATE (nBridget:User {name: 'Bridget'}) " +
            "CREATE (nCharles:User {name: 'Charles'}) " +

            "CREATE (nAlice)-[:LINK]->(nBridget) " +
            "CREATE (nBridget)-[:LINK]->(nCharles) ";

        String graphCreateQuery = "CALL gds.graph.create(" +
                                  "    'myGraph'," +
                                  "    'User'," +
                                  "    'LINK'," +
                                  "    {}" +
                                  ")";

        runQuery(dbQuery);
        runQuery(graphCreateQuery);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldCheckIsFinite() {
        String query = " UNWIND [1.0, gds.util.NaN(), gds.util.infinity()] as value" +
                       " RETURN gds.util.isFinite(value) as isFinite";

        String expected = "+----------+\n" +
                          "| isFinite |\n" +
                          "+----------+\n" +
                          "| true     |\n" +
                          "| false    |\n" +
                          "| false    |\n" +
                          "+----------+\n" +
                          "3 rows\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldCheckIsInFinite() {
        String query = " UNWIND [1.0, gds.util.NaN(), gds.util.infinity()] as value" +
                       " RETURN gds.util.isInfinite(value) as isInfinite";

        String expected = "+------------+\n" +
                          "| isInfinite |\n" +
                          "+------------+\n" +
                          "| false      |\n" +
                          "| true       |\n" +
                          "| true       |\n" +
                          "+------------+\n" +
                          "3 rows\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldReturnPath() {
        String query = " MATCH (u1)-->(u2)-->(u3)" +
                       " WITH [id(u1), id(u2), id(u3)] as nodeIds" +
                       " RETURN gds.util.asPath(nodeIds) as path" ;

        String expected = "+-----------------------------------+\n" +
                          "| path                              |\n" +
                          "+-----------------------------------+\n" +
                          "| (0)-[-1:NEXT]->(1)-[-2:NEXT]->(2) |\n" +
                          "+-----------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Disabled
    @Test
    void shouldReturnPathWithWeights() {
        String query = " MATCH (u1)->(u2)->(u3)\n" +
                       " WITH collect(id(u1), id(u2), id(u3)) as nodeIds, [2, 4] as weights\n" +
                       " RETURN gds.util.asPath(nodeIds, weights, true) as path" ;

        String expected = "+-----------------------------------+\n" +
                          "| path                              |\n" +
                          "+-----------------------------------+\n" +
                          "| (0)-[-1:NEXT]->(1)-[-2:NEXT]->(2) |\n" +
                          "+-----------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }
}
