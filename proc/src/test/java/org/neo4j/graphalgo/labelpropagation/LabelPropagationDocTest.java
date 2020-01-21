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
package org.neo4j.graphalgo.labelpropagation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LabelPropagationDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );

        String createGraph =
            "CREATE (alice:User {name: 'Alice', seed_label: 52})" +
            "CREATE (bridget:User {name: 'Bridget', seed_label: 21})" +
            "CREATE (charles:User {name: 'Charles', seed_label: 43})" +
            "CREATE (doug:User {name: 'Doug', seed_label: 21})" +
            "CREATE (mark:User {name: 'Mark', seed_label: 19})" +
            "CREATE (michael:User {name: 'Michael', seed_label: 52})" +
            "" +
            "CREATE (alice)-[:FOLLOW {weight: 1}]->(bridget)" +
            "CREATE (alice)-[:FOLLOW {weight: 10}]->(charles)" +
            "CREATE (mark)-[:FOLLOW {weight: 1}]->(doug)" +
            "CREATE (bridget)-[:FOLLOW {weight: 1}]->(michael)" +
            "CREATE (doug)-[:FOLLOW {weight: 1}]->(mark)" +
            "CREATE (michael)-[:FOLLOW {weight: 1}]->(alice)" +
            "CREATE (alice)-[:FOLLOW {weight: 1}]->(michael)" +
            "CREATE (bridget)-[:FOLLOW {weight: 1}]->(alice)" +
            "CREATE (michael)-[:FOLLOW {weight: 1}]->(bridget)" +
            "CREATE (charles)-[:FOLLOW {weight: 1}]->(doug)";

        runQuery(createGraph);
        registerProcedures(LabelPropagationWriteProc.class, LabelPropagationStreamProc.class, GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }


    @Test
    void unweighted() {
        String q1 = "CALL gds.labelPropagation.stream({" +
                    "   nodeProjection: 'User'," +
                    "   relationshipProjection: 'FOLLOW'" +
                    "})" +
                    "YIELD nodeId, communityId AS Community " +
                    "RETURN gds.util.asNode(nodeId).name AS Name, Community " +
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

        assertEquals(expectedString, runQuery(q1, Result::resultAsString));

        expectedString = "+--------------------------------+\n" +
                         "| ranIterations | communityCount |\n" +
                         "+--------------------------------+\n" +
                         "| 3             | 2              |\n" +
                         "+--------------------------------+\n" +
                         "1 row\n";

        String q2 = "CALL gds.labelPropagation.write({" +
                    "   nodeProjection: 'User'," +
                    "   relationshipProjection: 'FOLLOW'," +
                    "   writeProperty: 'community'" +
                    "})" +
                    "YIELD ranIterations, communityCount";

        assertEquals(expectedString, runQuery(q2, Result::resultAsString));
    }

    @Test
    void weighted() {
        String q1 = "CALL gds.labelPropagation.stream({" +
                    "   nodeProjection: 'User'," +
                    "   relationshipProjection: 'FOLLOW'," +
                    "   relationshipProperties: 'weight'," +
                    "   relationshipWeightProperty: 'weight'" +
                    "})" +
                    "YIELD nodeId, communityId AS Community " +
                    "RETURN gds.util.asNode(nodeId).name AS Name, Community " +
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

        assertEquals(expectedString, runQuery(q1, Result::resultAsString));

        String q2 = "CALL gds.labelPropagation.write({" +
                    "   nodeProjection: 'User'," +
                    "   relationshipProjection: 'FOLLOW'," +
                    "   relationshipProperties: 'weight'," +
                    "   writeProperty: 'community'," +
                    "   relationshipWeightProperty: 'weight'" +
                    "})" +
                    "YIELD ranIterations, communityCount";

        expectedString = "+--------------------------------+\n" +
                         "| ranIterations | communityCount |\n" +
                         "+--------------------------------+\n" +
                         "| 4             | 2              |\n" +
                         "+--------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, runQuery(q2, Result::resultAsString));
    }

    @Test
    void seeded(){
        String q1 = "CALL gds.labelPropagation.stream({" +
                    "   nodeProjection: 'User'," +
                    "   relationshipProjection: 'FOLLOW'," +
                    "   nodeProperties: 'seed_label'," +
                    "   seedProperty: 'seed_label'" +
                    "})" +
                    "YIELD nodeId, communityId AS Community " +
                    "RETURN gds.util.asNode(nodeId).name AS Name, Community " +
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

        assertEquals(expectedString, runQuery(q1, Result::resultAsString));

        String q2 = "CALL gds.labelPropagation.write({" +
                    "   nodeProjection: 'User'," +
                    "   relationshipProjection: 'FOLLOW'," +
                    "   nodeProperties: 'seed_label'," +
                    "   writeProperty: 'community'," +
                    "   seedProperty: 'seed_label'" +
                    " })" +
                    "YIELD ranIterations, communityCount";

        expectedString = "+--------------------------------+\n" +
                         "| ranIterations | communityCount |\n" +
                         "+--------------------------------+\n" +
                         "| 3             | 2              |\n" +
                         "+--------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, runQuery(q2, Result::resultAsString));
    }

    // Queries from the named graph and Cypher projection example in label-propagation.adoc
    // used to test that the results are correct in the docs
    @Test
    void namedGraphAndCypherProjection() {
        String loadGraph = "CALL  gds.graph.create('myGraph', 'User', 'FOLLOW')";
        runQuery(loadGraph);

        String q1 = "CALL gds.labelPropagation.stream('myGraph', {})" +
                    "YIELD nodeId, communityId AS Community " +
                    "RETURN gds.util.asNode(nodeId).name AS Name, Community " +
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

        assertEquals(expectedString, runQuery(q1, Result::resultAsString));

        String q2 = "CALL gds.labelPropagation.stream({" +
                    "  nodeQuery: 'MATCH (p:User) RETURN id(p) AS id'," +
                    "  relationshipQuery: 'MATCH (p1:User)-[f:FOLLOW]->(p2:User)" +
                    "   RETURN id(p1) AS source, id(p2) AS target'" +
                    "})" +
                    "YIELD nodeId, communityId AS Community " +
                    "RETURN gds.util.asNode(nodeId).name AS Name, Community " +
                    "ORDER BY Community, Name";

        assertEquals(expectedString, runQuery(q2, Result::resultAsString));
    }
}
