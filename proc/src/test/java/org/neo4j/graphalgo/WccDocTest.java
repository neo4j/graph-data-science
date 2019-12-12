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
    void seeding() {
        String q1 = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", "componentId")
            .addParameter("weightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodePropertiesWritten", "componentCount", "writeProperty");

        String r1 = runQuery(q1).resultAsString();
//        System.out.println(r1);

        String q2 = "MATCH (b:User {name: 'Bridget'}) " +
                    "CREATE (b)-[:LINK {weight: 2.0}]->(new:User {name: 'Mats'})";
        String r2 = runQuery(q2).resultAsString();
//        System.out.println(r2);

        String q3 = GdsCypher.call()
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

        q3 += " RETURN algo.asNode(nodeId).name AS name, componentId" +
              " ORDER BY componentId, name";

        String r3 = runQuery(q3).resultAsString();
//        System.out.println(r3);

        String q4 = GdsCypher.call()
            .withNodeLabel("User")
            .withNodeProperty("componentId")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .writeMode()
            .addParameter("seedProperty", "componentId")
            .addParameter("weightProperty", "weight")
            .addParameter("writeProperty", "componentId")
            .addParameter("threshold", 1.0D)
            .yields("nodePropertiesWritten", "componentCount", "writeProperty");

        String r4 = runQuery(q4).resultAsString();
//        System.out.println(r4);

        // graph end-state
        System.out.println(runQuery("MATCH (n) RETURN n").resultAsString());
    }

    // Queries from the named graph and Cypher projection example in wcc.adoc
    // used to test that the results are correct in the docs
    @Test
    void namedGraphAndCypherProjection() {
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

        String r2 = runQuery(q2).resultAsString();
//        System.out.println(r2);

        String q3 = "CALL gds.algo.wcc.stream({" +
                    "   nodeQuery: 'MATCH (u:User) RETURN id(u) AS id', " +
                    "   relationshipQuery: 'MATCH (u1:User)-[:LINK]->(u2:User) RETURN id(u1) AS source, id(u2) AS target'" +
                    "}) " +
                    "YIELD nodeId, componentId " +
                    "RETURN algo.asNode(nodeId).name AS name, componentId " +
                    "ORDER BY componentId, name";

        String r3 = runQuery(q3).resultAsString();
//        System.out.println(r3);
    }
}
