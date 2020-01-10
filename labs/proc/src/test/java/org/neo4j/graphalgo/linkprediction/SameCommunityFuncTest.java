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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SameCommunityFuncTest extends BaseProcTest {

    private static final String DB_CYPHER =
            "CREATE (mark:Person {name: 'Mark'})\n" +
            "SET mark.community=1, mark.partition = 4\n" +
            "CREATE (michael:Person {name: 'Michael'})\n" +
            "SET michael.community=2, michael.partition=4\n" +
            "CREATE (praveena:Person {name: 'Praveena'})\n" +
            "SET praveena.community=1\n" +
            "CREATE (jennifer:Person {name: 'Jennifer'})\n";

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase((builder) -> builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*"));
        registerFunctions(LinkPredictionFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void missingProperty() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Jennifer'})\n" +
                        "MATCH (p2:Person {name: 'Mark'})\n" +
                        "RETURN gds.alpha.linkprediction.sameCommunity(p1, p2) AS score, " +
                        "       0.0 AS cypherScore";

        Map<String, Object> node = runQuery(controlQuery, Result::next);
        assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
    }

    @Test
    void sameCommunity() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Praveena'})\n" +
                        "MATCH (p2:Person {name: 'Mark'})\n" +
                        "RETURN gds.alpha.linkprediction.sameCommunity(p1, p2) AS score, " +
                        "       1.0 AS cypherScore";

        Map<String, Object> node = runQuery(controlQuery, Result::next);
        assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
    }

    @Test
    void differentCommunity() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Michael'})\n" +
                        "MATCH (p2:Person {name: 'Mark'})\n" +
                        "RETURN gds.alpha.linkprediction.sameCommunity(p1, p2) AS score, " +
                        "       0.0 AS cypherScore";

        Map<String, Object> node = runQuery(controlQuery, Result::next);
        assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
    }

    @Test
    void specifyProperty() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Michael'})\n" +
                        "MATCH (p2:Person {name: 'Mark'})\n" +
                        "RETURN gds.alpha.linkprediction.sameCommunity(p1, p2, 'partition') AS score, " +
                        "       1.0 AS cypherScore";

        Map<String, Object> node = runQuery(controlQuery, Result::next);
        assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
    }
}
