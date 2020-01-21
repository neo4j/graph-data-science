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
package org.neo4j.graphalgo.linkprediction;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.similarity.SimilaritiesFunc;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JaccardFuncDocTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE  " +
        "  (french:Cuisine {name:'French'}) " +
        ", (italian:Cuisine {name:'Italian'}) " +
        ", (indian:Cuisine {name:'Indian'}) " +
        ", (lebanese:Cuisine {name:'Lebanese'}) " +
        ", (portuguese:Cuisine {name:'Portuguese'}) " +
        " " +
        ", (zhen:Person {name: 'Zhen'}) " +
        ", (praveena:Person {name: 'Praveena'}) " +
        ", (michael:Person {name: 'Michael'}) " +
        ", (arya:Person {name: 'Arya'}) " +
        ", (karin:Person {name: 'Karin'}) " +

        ", (praveena)-[:LIKES]->(indian) "+
        ", (praveena)-[:LIKES]->(portuguese) "+

        ",  (zhen)-[:LIKES]->(french) "+
        ", (zhen)-[:LIKES]->(indian) "+

        ", (michael)-[:LIKES]->(french) "+
        ", (michael)-[:LIKES]->(italian) "+
        ", (michael)-[:LIKES]->(indian) "+

        ", (arya)-[:LIKES]->(lebanese) "+
        ", (arya)-[:LIKES]->(italian) "+
        ", (arya)-[:LIKES]->(portuguese) "+

        ", (karin)-[:LIKES]->(lebanese) "+
        ", (karin)-[:LIKES]->(italian)";

    String NL = System.lineSeparator();

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        db.execute(DB_CYPHER);
        registerFunctions(SimilaritiesFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void functionCall() {
        String query = "RETURN gds.alpha.similarity.jaccard([1,2,3], [1,2,4,5]) AS similarity";

        String expectedString = "+------------+" + NL +
                                "| similarity |" + NL +
                                "+------------+" + NL +
                                "| 0.4        |" + NL +
                                "+------------+" + NL +
                                "1 row" + NL;
        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void functionCallOnCypher() {
        @Language("Cypher")
        String query = " MATCH (p1:Person {name: 'Karin'})-[:LIKES]->(cuisine1)" +
                       " WITH p1, collect(id(cuisine1)) AS p1Cuisine" +
                       " MATCH (p2:Person {name: 'Arya'})-[:LIKES]->(cuisine2)" +
                       " WITH p1, p1Cuisine, p2, collect(id(cuisine2)) AS p2Cuisine" +
                       " RETURN p1.name AS from," +
                       "       p2.name AS to," +
                       "       gds.alpha.similarity.jaccard(p1Cuisine, p2Cuisine) AS similarity";
        String expectedString = "+---------------------------------------+" + NL +
                                "| from    | to     | similarity         |" + NL +
                                "+---------------------------------------+" + NL +
                                "| \"Karin\" | \"Arya\" | 0.6666666666666666 |" + NL +
                                "+---------------------------------------+" + NL +
                                "1 row" + NL;
        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void functionOnAll() {
        @Language("Cypher")
        String query = " MATCH (p1:Person {name: 'Karin'})-[:LIKES]->(cuisine1)" +
                       " WITH p1, collect(id(cuisine1)) AS p1Cuisine" +
                       " MATCH (p2:Person)-[:LIKES]->(cuisine2) WHERE p1 <> p2" +
                       " WITH p1, p1Cuisine, p2, collect(id(cuisine2)) AS p2Cuisine" +
                       " RETURN p1.name AS from," +
                       "       p2.name AS to," +
                       "       gds.alpha.similarity.jaccard(p1Cuisine, p2Cuisine) AS similarity" +
                       " ORDER BY to, similarity DESC";

        String expectedString = "+-------------------------------------------+" + NL +
                                "| from    | to         | similarity         |" + NL +
                                "+-------------------------------------------+" + NL +
                                "| \"Karin\" | \"Arya\"     | 0.6666666666666666 |" + NL +
                                "| \"Karin\" | \"Michael\"  | 0.25               |" + NL +
                                "| \"Karin\" | \"Praveena\" | 0.0                |" + NL +
                                "| \"Karin\" | \"Zhen\"     | 0.0                |" + NL +
                                "+-------------------------------------------+" + NL +
                                "4 rows" + NL;
        assertEquals(expectedString, db.execute(query).resultAsString());
    }
}