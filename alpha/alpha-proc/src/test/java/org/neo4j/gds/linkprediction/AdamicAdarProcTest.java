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
package org.neo4j.gds.linkprediction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdamicAdarProcTest extends BaseProcTest {

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfigRaw(Map.of("unsupported.dbms.debug.track_cursor_close", "false"));
        builder.setConfigRaw(Map.of("unsupported.dbms.debug.trace_cursors", "false"));
    }

    private static final String DB_CYPHER =
        "CREATE " +
        " (mark:Person {name: 'Mark'}), " +
        " (michael:Person {name: 'Michael'}), " +
        " (praveena:Person {name: 'Praveena'}), " +
        " (ryan:Person {name: 'Ryan'}), " +
        " (karin:Person {name: 'Karin'}), " +
        " (jennifer:Person {name: 'Jennifer'}), " +
        " (elaine:Person {name: 'Elaine'}), " +

        " (jennifer)-[:FRIENDS]->(ryan), " +
        " (jennifer)-[:FRIENDS]->(karin), " +
        " (elaine)-[:FRIENDS]->(ryan), " +
        " (elaine)-[:FRIENDS]->(karin), " +

        " (mark)-[:FRIENDS]->(michael), " +
        " (mark)-[:WORKS_WITH]->(michael), " +

        " (praveena)-[:FRIENDS]->(michael)";

    @BeforeEach
    void setUp() throws Exception {
        registerFunctions(LinkPredictionFunc.class);
        runQuery(DB_CYPHER);
    }

    @Test
    void oneNodeInCommon() {
        String controlQuery =
            "MATCH (p1:Person {name: 'Mark'}) " +
            "MATCH (p2:Person {name: 'Praveena'}) " +
            "RETURN gds.alpha.linkprediction.adamicAdar(p1, p2) AS score, " +
            "       1/log(3) AS cypherScore";

        Map<String, Object> node = runQuery(controlQuery, Result::next);
        assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
    }

    @Test
    void oneNodeInCommonExplicit() {
        String controlQuery =
            "MATCH (p1:Person {name: 'Mark'}) " +
            "MATCH (p2:Person {name: 'Praveena'}) " +
            "RETURN gds.alpha.linkprediction.adamicAdar(p1, p2, " +
            "{relationshipQuery: 'FRIENDS', direction: 'BOTH'}) AS score," +
            "1/log(2) AS cypherScore";

        Map<String, Object> node = runQuery(controlQuery, Result::next);
        assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
    }

    @Test
    void twoNodesInCommon() {
        String controlQuery =
            "MATCH (p1:Person {name: 'Jennifer'}) " +
            "MATCH (p2:Person {name: 'Elaine'}) " +
            "RETURN gds.alpha.linkprediction.adamicAdar(p1, p2) AS score, " +
            "       1/log(2) + 1/log(2) AS cypherScore";

        Map<String, Object> node = runQuery(controlQuery, Result::next);
        assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
    }

    @Test
    void noNeighbors() {
        String controlQuery =
            "MATCH (p1:Person {name: 'Jennifer'}) " +
            "MATCH (p2:Person {name: 'Ryan'}) " +
            "RETURN gds.alpha.linkprediction.adamicAdar(p1, p2) AS score, " +
            "       0.0 AS cypherScore";

        Map<String, Object> node = runQuery(controlQuery, Result::next);
        assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
    }

    @Test
    void bothNodesTheSame() {
        String controlQuery =
            "MATCH (p1:Person {name: 'Praveena'}) " +
            "MATCH (p2:Person {name: 'Praveena'}) " +
            "RETURN gds.alpha.linkprediction.adamicAdar(p1, p2) AS score, " +
            "       0.0 AS cypherScore";

        Map<String, Object> node = runQuery(controlQuery, Result::next);
        assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
    }
}
