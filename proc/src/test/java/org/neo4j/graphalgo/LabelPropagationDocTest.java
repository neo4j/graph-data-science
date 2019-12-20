/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LabelPropagationDocTest extends ProcTestBase {

    @BeforeEach
    void setup() throws KernelException {
        String createGraph =
            "CREATE (nAlice:User {name:'Alice', seed_label:52})" +
            "CREATE (nBridget:User {name:'Bridget', seed_label:21})" +
            "CREATE (nCharles:User {name:'Charles', seed_label:43})" +
            "CREATE (nDoug:User {name:'Doug', seed_label:21})" +
            "CREATE (nMark:User {name:'Mark', seed_label:19})" +
            "CREATE (nMichael:User {name:'Michael', seed_label:52})" +

            "CREATE (nAlice)-[:FOLLOW {weight:1}]->(nBridget)" +
            "CREATE (nAlice)-[:FOLLOW {weight:10}]->(nCharles)" +
            "CREATE (nMark)-[:FOLLOW {weight:1}]->(nDoug)" +
            "CREATE (nBridget)-[:FOLLOW {weight:1}]->(nMichael)" +
            "CREATE (nDoug)-[:FOLLOW {weight:1}]->(nMark)" +
            "CREATE (nMichael)-[:FOLLOW {weight:1}]->(nAlice)" +
            "CREATE (nAlice)-[:FOLLOW {weight:1}]->(nMichael)" +
            "CREATE (nBridget)-[:FOLLOW {weight:1}]->(nAlice)" +
            "CREATE (nMichael)-[:FOLLOW {weight:1}]->(nBridget)" +
            "CREATE (nCharles)-[:FOLLOW {weight:1}]->(nDoug);";

        db = TestDatabaseCreator.createTestDatabase(builder ->
                builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "algo.*")
        );
        db.execute(createGraph);
        registerProcedures(LabelPropagationProc.class, GraphLoadProc.class);
        registerFunctions(GetNodeFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }


    @Test
    void unweighted() {
        String q1 = "CALL algo.labelPropagation.stream('User', 'FOLLOW', {" +
                    "  direction: 'OUTGOING'," +
                    "  iterations: 10" +
                    "})" +
                    "YIELD nodeId, label AS Community " +
                    "RETURN algo.asNode(nodeId).name AS Name, Community " +
                    "ORDER BY Community, Name";

        String expectedString = "+-----------------------+\n" +
                                "| Name      | Community |\n" +
                                "+-----------------------+\n" +
                                "| \"Alice\"   | 1         |\n" +
                                "| \"Bridget\" | 1         |\n" +
                                "| \"Michael\" | 1         |\n" +
                                "| \"Charles\" | 4         |\n" +
                                "| \"Doug\"    | 4         |\n" +
                                "| \"Mark\"    | 4         |\n" +
                                "+-----------------------+\n" +
                                "6 rows\n";

        assertEquals(expectedString, db.execute(q1).resultAsString());

        expectedString = "+-----------------------------------------------------+\n" +
                         "| nodes | iterations | communityCount | writeProperty |\n" +
                         "+-----------------------------------------------------+\n" +
                         "| 6     | 3          | 2              | \"community\"   |\n" +
                         "+-----------------------------------------------------+\n" +
                         "1 row\n";

        String q2 = "CALL algo.labelPropagation('User', 'FOLLOW', {" +
                    "  iterations: 10," +
                    "  writeProperty: 'community'," +
                    "  write: true," +
                    "  direction: 'OUTGOING'" +
                    "})" +
                    "YIELD nodes, iterations, communityCount, writeProperty;";

        assertEquals(expectedString, db.execute(q2).resultAsString());
    }

    @Test
    void weighted() {
        String q1 = "CALL algo.labelPropagation.stream('User', 'FOLLOW', {" +
                    "  direction: 'OUTGOING'," +
                    "  iterations: 10," +
                    "  weightProperty: 'weight'" +
                    "})" +
                    "YIELD nodeId, label AS Community " +
                    "RETURN algo.asNode(nodeId).name AS Name, Community " +
                    "ORDER BY Community, Name";

        String expectedString = "+-----------------------+\n" +
                                "| Name      | Community |\n" +
                                "+-----------------------+\n" +
                                "| \"Bridget\" | 2         |\n" +
                                "| \"Michael\" | 2         |\n" +
                                "| \"Alice\"   | 4         |\n" +
                                "| \"Charles\" | 4         |\n" +
                                "| \"Doug\"    | 4         |\n" +
                                "| \"Mark\"    | 4         |\n" +
                                "+-----------------------+\n" +
                                "6 rows\n";

        assertEquals(expectedString, db.execute(q1).resultAsString());

        String q2 = "CALL algo.labelPropagation('User', 'FOLLOW', {" +
                    "  iterations: 10," +
                    "  writeProperty: 'community'," +
                    "  write: true," +
                    "  direction: 'OUTGOING'," +
                    "  weightProperty: 'weight'" +
                    "})" +
                    "YIELD nodes, iterations, communityCount, writeProperty, weightProperty;";

        expectedString = "+----------------------------------------------------------------------+\n" +
                         "| nodes | iterations | communityCount | writeProperty | weightProperty |\n" +
                         "+----------------------------------------------------------------------+\n" +
                         "| 6     | 4          | 2              | \"community\"   | \"weight\"       |\n" +
                         "+----------------------------------------------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, db.execute(q2).resultAsString());
    }

    @Test
    void seeded(){
        String q1 = "CALL algo.labelPropagation.stream('User', 'FOLLOW', {" +
                    "  iterations: 10," +
                    "  seedProperty: 'seed_label'," +
                    "  direction: 'OUTGOING'" +
                    "})" +
                    "YIELD nodeId, label AS Community " +
                    "RETURN algo.asNode(nodeId).name AS Name, Community " +
                    "ORDER BY Community, Name";

        String expectedString = "+-----------------------+\n" +
                                "| Name      | Community |\n" +
                                "+-----------------------+\n" +
                                "| \"Charles\" | 19        |\n" +
                                "| \"Doug\"    | 19        |\n" +
                                "| \"Mark\"    | 19        |\n" +
                                "| \"Alice\"   | 21        |\n" +
                                "| \"Bridget\" | 21        |\n" +
                                "| \"Michael\" | 21        |\n" +
                                "+-----------------------+\n" +
                                "6 rows\n";

        assertEquals(expectedString, db.execute(q1).resultAsString());

        String q2 = "CALL algo.labelPropagation('User', 'FOLLOW', {" +
                    "  direction: 'OUTGOING'," +
                    "  iterations: 10," +
                    "  seedProperty: 'seed_label'," +
                    "  writeProperty: 'community'," +
                    "  write: true" +
                    " })" +
                    "YIELD nodes, iterations, communityCount, writeProperty;";

        expectedString = "+-----------------------------------------------------+\n" +
                         "| nodes | iterations | communityCount | writeProperty |\n" +
                         "+-----------------------------------------------------+\n" +
                         "| 6     | 3          | 2              | \"community\"   |\n" +
                         "+-----------------------------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, db.execute(q2).resultAsString());
    }

    // Queries from the named graph and Cypher projection example in label-propagation.adoc
    // used to test that the results are correct in the docs
    @Test
    void namedGraphAndCypherProjection() {
        String loadGraph = "CALL algo.graph.load('myGraph', 'User', 'FOLLOW');";
        db.execute(loadGraph);

        String q1 = "CALL algo.labelPropagation.stream(null, null, {" +
                    "  graph: 'myGraph'," +
                    "  direction: 'OUTGOING'," +
                    "  iterations: 10" +
                    "})" +
                    "YIELD nodeId, label " +
                    "RETURN algo.asNode(nodeId).name AS Name, label AS Community " +
                    "ORDER BY Community, Name;";

        String expectedString = "+-----------------------+\n" +
                                "| Name      | Community |\n" +
                                "+-----------------------+\n" +
                                "| \"Alice\"   | 1         |\n" +
                                "| \"Bridget\" | 1         |\n" +
                                "| \"Michael\" | 1         |\n" +
                                "| \"Charles\" | 4         |\n" +
                                "| \"Doug\"    | 4         |\n" +
                                "| \"Mark\"    | 4         |\n" +
                                "+-----------------------+\n" +
                                "6 rows\n";

        assertEquals(expectedString, db.execute(q1).resultAsString());

        String q2 = "CALL algo.labelPropagation.stream(" +
                    "  'MATCH (p:User) RETURN id(p) AS id'," +
                    "  'MATCH (p1:User)-[f:FOLLOW]->(p2:User)" +
                    "   RETURN id(p1) AS source', {" +
                    "  graph: 'cypher'," +
                    "  direction: 'OUTGOING'," +
                    "  iterations: 10" +
                    "})" +
                    "YIELD nodeId, label " +
                    "RETURN algo.asNode(nodeId).name AS Name, label AS Community " +
                    "ORDER BY Community, Name;";

        assertEquals(expectedString, db.execute(q2).resultAsString());
    }
}
