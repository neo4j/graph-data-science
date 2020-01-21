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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.walking.RandomWalkProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RandomWalkDocTest extends BaseProcTest {

    public static final String NL = System.lineSeparator();

    public static final String DB_CYPHER =
        "CREATE" +
        "  (home:Page {name: 'Home'})" +
        ", (about:Page {name: 'About'})" +
        ", (product:Page {name: 'Product'})" +
        ", (links:Page {name: 'Links'})" +
        ", (a:Page {name: 'Site A'})" +
        ", (b:Page {name: 'Site B'})" +
        ", (c:Page {name: 'Site C'})" +
        ", (d:Page {name: 'Site D'})" +
        ", (home)-[:LINKS]->(about)" +
        ", (about)-[:LINKS]->(home)" +
        ", (product)-[:LINKS]->(home)" +
        ", (home)-[:LINKS]->(product)" +
        ", (links)-[:LINKS]->(home)" +
        ", (home)-[:LINKS]->(links)" +
        ", (links)-[:LINKS]->(a)" +
        ", (a)-[:LINKS]->(home)" +
        ", (links)-[:LINKS]->(b)" +
        ", (b)-[:LINKS]->(home)" +
        ", (links)-[:LINKS]->(c)" +
        ", (c)-[:LINKS]->(home)" +
        ", (links)-[:LINKS]->(d)" +
        ", (d)-[:LINKS]->(home)";

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase((builder) ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.util.*")
        );
        registerProcedures(RandomWalkProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void should() {
        @Language("Cypher")
        String query = " MATCH (home:Page {name: 'Home'})" +
                       " CALL gds.alpha.randomWalk.stream({" +
                       "   relationshipProjection: {" +
                       "     LINKS: {" +
                       "       type: 'LINKS'," +
                       "       projection: 'UNDIRECTED'" +
                       "     }" +
                       "   }," +
                       "   start: id(home)," +
                       "   steps: 3," +
                       "   walks: 1" +
                       " })" +
                       " YIELD nodeIds" +
                       " UNWIND nodeIds AS nodeId" +
                       " RETURN gds.util.asNode(nodeId).name AS page";

        String expected = "+----------+" + NL +
                          "| page     |" + NL +
                          "+----------+" + NL +
                          "| \"Home\"   |" + NL +
                          "| \"Links\"  |" + NL +
                          "| \"Site A\" |" + NL +
                          "| \"Home\"   |" + NL +
                          "+----------+" + NL +
                          "4 rows" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected.split(NL).length, actual.split(NL).length);
    }

    @Test
    void shouldCypher() {
        @Language("Cypher")
        String query = " MATCH (home:Page {name: 'Home'})" +
                       " CALL gds.alpha.randomWalk.stream({" +
                       "   nodeQuery: 'MATCH (p:Page) RETURN id(p) AS id'," +
                       "   relationshipQuery: 'MATCH (p1:Page)-[:LINKS]->(p2:Page) RETURN id(p1) AS source, id(p2) AS target'," +
                       "   start: id(home)," +
                       "   steps: 5," +
                       "   walks: 1" +
                       " })" +
                       " YIELD nodeIds" +
                       " UNWIND nodeIds AS nodeId" +
                       " RETURN gds.util.asNode(nodeId).name AS page";

        String expected = "+-----------+" + NL +
                          "| page      |" + NL +
                          "+-----------+" + NL +
                          "| \"Home\"    |" + NL +
                          "| \"Product\" |" + NL +
                          "| \"Home\"    |" + NL +
                          "| \"Product\" |" + NL +
                          "| \"Home\"    |" + NL +
                          "| \"Product\" |" + NL +
                          "+-----------+" + NL +
                          "6 rows" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected.split(NL).length, actual.split(NL).length);
    }
}
