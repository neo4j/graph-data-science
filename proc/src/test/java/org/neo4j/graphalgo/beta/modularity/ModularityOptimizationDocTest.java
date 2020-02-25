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
package org.neo4j.graphalgo.beta.modularity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ModularityOptimizationDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();

        registerProcedures(ModularityOptimizationWriteProc.class, ModularityOptimizationStreamProc.class);
        registerProcedures(GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class);

        String dbQuery = "CREATE" +
                         "  (a:Person {name:'Alice'})" +
                         ", (b:Person {name:'Bridget'})" +
                         ", (c:Person {name:'Charles'})" +
                         ", (d:Person {name:'Doug'})" +
                         ", (e:Person {name:'Elton'})" +
                         ", (f:Person {name:'Frank'})" +
                         ", (a)-[:KNOWS {weight: 0.01}]->(b)" +
                         ", (a)-[:KNOWS {weight: 5.0}]->(e)" +
                         ", (a)-[:KNOWS {weight: 5.0}]->(f)" +
                         ", (b)-[:KNOWS {weight: 5.0}]->(c)" +
                         ", (b)-[:KNOWS {weight: 5.0}]->(d)" +
                         ", (c)-[:KNOWS {weight: 0.01}]->(e)" +
                         ", (f)-[:KNOWS {weight: 0.01}]->(d)";

        String graphCreateQuery = "CALL gds.graph.create(" +
                                  "'myGraph', " +
                                  "'Person', " +
                                  "{" +
                                  " KNOWS: {" +
                                  "     type: 'KNOWS'," +
                                  "     orientation: 'UNDIRECTED'," +
                                  "     properties: ['weight']" +
                                  " }" +
                                  "})";

        runQuery(dbQuery);
        runQuery(graphCreateQuery);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
        db.shutdown();
    }

    @Test
    void shouldStream() {
        String query = "CALL gds.beta.modularityOptimization.stream('myGraph', { relationshipWeightProperty: 'weight' }) " +
                       "YIELD nodeId, communityId " +
                       "RETURN gds.util.asNode(nodeId).name AS name, communityId " +
                       "ORDER BY name";

        String actual = runQuery(query, Result::resultAsString);
        String expected =
            "+-------------------------+" + NL +
            "| name      | communityId |" + NL +
            "+-------------------------+" + NL +
            "| \"Alice\"   | 4           |" + NL +
            "| \"Bridget\" | 1           |" + NL +
            "| \"Charles\" | 1           |" + NL +
            "| \"Doug\"    | 1           |" + NL +
            "| \"Elton\"   | 4           |" + NL +
            "| \"Frank\"   | 4           |" + NL +
            "+-------------------------+" + NL +
            "6 rows" + NL;

        assertEquals(expected, actual);
    }
    @Test
    void shouldWrite() {
        String query = "CALL gds.beta.modularityOptimization.write('myGraph', { relationshipWeightProperty: 'weight', writeProperty: 'community' })" +
                       "YIELD nodes, communityCount, ranIterations, didConverge";

        String actual = runQuery(query, Result::resultAsString);
        String expected =
            "+------------------------------------------------------+" + NL +
            "| nodes | communityCount | ranIterations | didConverge |" + NL +
            "+------------------------------------------------------+" + NL +
            "| 6     | 2              | 3             | true        |" + NL +
            "+------------------------------------------------------+" + NL +
            "1 row" + NL;

        assertEquals(expected, actual);
    }
}
