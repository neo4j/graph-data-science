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
import org.neo4j.graphalgo.newapi.GraphCatalogProcs;
import org.neo4j.graphalgo.wcc.WccStreamProc;
import org.neo4j.graphalgo.wcc.WccWriteProc;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

class WccDocTest extends ProcTestBase {

    @BeforeEach
    void setup() throws Exception {
        String createGraph = "CREATE (nAlice:User {name: 'Alice'}) " +
                             "CREATE (nBridget:User {name: 'Bridget'}) " +
                             "CREATE (nCharles:User {name: 'Charles'}) " +
                             "CREATE (nDoug:User {name: 'Doug'}) " +
                             "CREATE (nMark:User {name: 'Mark'}) " +
                             "CREATE (nMichael:User {name: 'Michael'}) " +

                             "CREATE (nAlice)-[:LINK {weight: 0.5}]->(nBridget) " +
                             "CREATE (nAlice)-[:LINK {weight: 4}]->(nCharles) " +
                             "CREATE (nMark)-[:LINK {weight: 1.1}]->(nDoug) " +
                             "CREATE (nMark)-[:LINK {weight: 2}]->(nMichael); ";

        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "algo.*")
        );
        runQuery(createGraph);
        registerProcedures(WccStreamProc.class, WccWriteProc.class, GraphCatalogProcs.class);
        registerFunctions(GetNodeFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    // Queries and results match wcc.adoc Seeding section; should read from there in a future
    // Doesn't have any assertions; those should be to verify results with contents in wcc.adoc
    // This is left for a future task
    @Test
    void implicitGraph() {
        // stream
        String q0 = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .algo("wcc")
            .streamMode()
            .yields("nodeId", "componentId");

        q0 += " RETURN algo.asNode(nodeId).name AS name, componentId" +
              " ORDER BY componentId, name";

//        System.out.println("q0 = " + q0);
        String r0 = runQuery(q0).resultAsString();
//        System.out.println(r0);

        // write
        String q1 = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", "componentId")
            .yields("nodePropertiesWritten", "componentCount", "writeProperty");

//        System.out.println("q1 = " + q1);
        String r1 = runQuery(q1).resultAsString();
//        System.out.println(r1);

        // stream weighted
        String q2 = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .streamMode()
            .addParameter("weightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodeId", "componentId");

        q2 += " RETURN algo.asNode(nodeId).name AS name, componentId" +
              " ORDER BY componentId, name";

//        System.out.println("q2 = " + q2);
        String r2 = runQuery(q2).resultAsString();
//        System.out.println(r2);

        // write weighted
        String q3 = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", "componentId")
            .addParameter("weightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodePropertiesWritten", "componentCount", "writeProperty");

//        System.out.println("q3 = " + q3);
        String r3 = runQuery(q3).resultAsString();
//        System.out.println(r3);

        // create new node and relationship
        String q4 = "MATCH (b:User {name: 'Bridget'}) " +
                    "CREATE (b)-[:LINK {weight: 2.0}]->(new:User {name: 'Mats'})";
        System.out.println("q4 = " + q4);
        String r4 = runQuery(q4).resultAsString();
        System.out.println(r4);

        // stream with seeding
        String q5 = GdsCypher.call()
            .withNodeLabel("User")
            .withNodeProperty("componentId")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .streamMode()
            .addParameter("seedProperty", "componentId")
            .addParameter("weightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodeId", "componentId");

        q5 += " RETURN algo.asNode(nodeId).name AS name, componentId" +
              " ORDER BY componentId, name";

//        System.out.println("q5 = " + q5);
        String r5 = runQuery(q5).resultAsString();
//        System.out.println(r5);

        // write with seeding
        String q6 = GdsCypher.call()
            .withNodeLabel("User")
            .withNodeProperty("componentId")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .writeMode()
            .addParameter("seedProperty", "componentId")
            .addParameter("writeProperty", "componentId")
            .addParameter("weightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodePropertiesWritten", "componentCount", "writeProperty");

//        System.out.println("q6 = " + q6);
        String r6 = runQuery(q6).resultAsString();
//        System.out.println(r6);
    }

    // Queries from the named graph and Cypher projection example in wcc.adoc
    // used to test that the results are correct in the docs
    @Test
    void namedGraph() {
        String q1 = "CALL algo.beta.graph.create('myGraph', ['User'], ['LINK']) YIELD graphName;";
        String r1 = runQuery(q1).resultAsString();
//        System.out.println(r1);

        String q2 = GdsCypher.call()
            .explicitCreation("myGraph")
            .algo("wcc")
            .streamMode()
            .yields("nodeId", "componentId");

        q2 += " RETURN algo.asNode(nodeId).name AS name, componentId" +
              " ORDER BY componentId, name;";

//        System.out.println("q2 = " + q2);
        String r2 = runQuery(q2).resultAsString();
//        System.out.println(r2);
    }

    @Test
    void cypherProjection() {
        String q0 = "CALL gds.algo.wcc.stream({" +
                    "   nodeQuery: 'MATCH (u:User) RETURN id(u) AS id', " +
                    "   relationshipQuery: 'MATCH (u1:User)-[:LINK]->(u2:User) RETURN id(u1) AS source, id(u2) AS target'" +
                    "}) " +
                    "YIELD nodeId, componentId " +
                    "RETURN algo.asNode(nodeId).name AS name, componentId " +
                    "ORDER BY componentId, name";

//        System.out.println("q0 = " + q0);
        String r0 = runQuery(q0).resultAsString();
//        System.out.println(r0);
    }
}
