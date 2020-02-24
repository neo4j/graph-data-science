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
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OneHotEncodingDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();

    private static final String DB_CYPHER =
        "CREATE" +
        "  (french:Cuisine {name:'French'})" +
        ", (italian:Cuisine {name:'Italian'})" +
        ", (indian:Cuisine {name:'Indian'})" +
        ", (zhen:Person {name: 'Zhen'})" +
        ", (praveena:Person {name: 'Praveena'})" +
        ", (michael:Person {name: 'Michael'})" +
        ", (arya:Person {name: 'Arya'})" +
        ", (praveena)-[:LIKES]->(indian)" +
        ", (zhen)-[:LIKES]->(french)" +
        ", (michael)-[:LIKES]->(french)" +
        ", (michael)-[:LIKES]->(italian)";

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerFunctions(OneHotEncodingFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void should1() {
        String query = "RETURN gds.alpha.ml.oneHotEncoding(['Chinese', 'Indian', 'Italian'], ['Italian']) AS embedding";

        String expected = "+-----------+" + NL +
                          "| embedding |" + NL +
                          "+-----------+" + NL +
                          "| [0,0,1]   |" + NL +
                          "+-----------+" + NL +
                          "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

    @Test
    void should2() {
        String query = " MATCH (cuisine:Cuisine)" +
                       " WITH cuisine" +
                       "   ORDER BY cuisine.name" +
                       " WITH collect(cuisine) AS cuisines" +
                       " MATCH (p:Person)" +
                       " RETURN p.name AS name, gds.alpha.ml.oneHotEncoding(cuisines, [(p)-[:LIKES]->(cuisine) | cuisine]) AS embedding" +
                       "   ORDER BY name";

        String expected = "+------------------------+" + NL +
                          "| name       | embedding |" + NL +
                          "+------------------------+" + NL +
                          "| \"Arya\"     | [0,0,0]   |" + NL +
                          "| \"Michael\"  | [1,0,1]   |" + NL +
                          "| \"Praveena\" | [0,1,0]   |" + NL +
                          "| \"Zhen\"     | [1,0,0]   |" + NL +
                          "+------------------------+" + NL +
                          "4 rows" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }
}
