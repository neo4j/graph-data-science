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
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SameCommunityDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();

    private static final String DB_CYPHER =
        "CREATE" +
        "  (zhen:Person {name: 'Zhen', community: 1})" +
        ", (praveena:Person {name: 'Praveena', community: 2})" +
        ", (michael:Person {name: 'Michael', community: 1})" +
        ", (arya:Person {name: 'Arya', partition: 5})" +
        ", (karin:Person {name: 'Karin', partition: 5})" +
        ", (jennifer:Person {name: 'Jennifer'})";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        db.execute(DB_CYPHER);
        registerFunctions(LinkPredictionFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void sameCommunity1() {
        @Language("Cypher")
        String query = " MATCH (p1:Person {name: 'Michael'})" +
                       " MATCH (p2:Person {name: 'Zhen'})" +
                       " RETURN gds.alpha.linkprediction.sameCommunity(p1, p2) AS score";

        String expectedString = "+-------+" + NL +
                                "| score |" + NL +
                                "+-------+" + NL +
                                "| 1.0   |" + NL +
                                "+-------+" + NL +
                                "1 row" + NL;

        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void sameCommunity2() {
        @Language("Cypher")
        String query = " MATCH (p1:Person {name: 'Michael'})" +
                       " MATCH (p2:Person {name: 'Praveena'})" +
                       " RETURN gds.alpha.linkprediction.sameCommunity(p1, p2) AS score";

        String expectedString = "+-------+" + NL +
                                "| score |" + NL +
                                "+-------+" + NL +
                                "| 0.0   |" + NL +
                                "+-------+" + NL +
                                "1 row" + NL;

        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void sameCommunity3() {
        @Language("Cypher")
        String query = " MATCH (p1:Person {name: 'Michael'})" +
                       " MATCH (p2:Person {name: 'Jennifer'})" +
                       " RETURN gds.alpha.linkprediction.sameCommunity(p1, p2) AS score";

        String expectedString = "+-------+" + NL +
                                "| score |" + NL +
                                "+-------+" + NL +
                                "| 0.0   |" + NL +
                                "+-------+" + NL +
                                "1 row" + NL;

        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void sameCommunity4() {
        @Language("Cypher")
        String query = " MATCH (p1:Person {name: 'Arya'})" +
                       " MATCH (p2:Person {name: 'Karin'})" +
                       " RETURN gds.alpha.linkprediction.sameCommunity(p1, p2, 'partition') AS score";

        String expectedString = "+-------+" + NL +
                                "| score |" + NL +
                                "+-------+" + NL +
                                "| 1.0   |" + NL +
                                "+-------+" + NL +
                                "1 row" + NL;

        assertEquals(expectedString, db.execute(query).resultAsString());
    }
}

