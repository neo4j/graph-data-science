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
import org.neo4j.graphalgo.unionfind.UnionFindProc;
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
        db.execute(createGraph);
        registerProcedures(UnionFindProc.class, GraphLoadProc.class);
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
        String q1 =
                "CALL algo.unionFind('User', 'LINK', { " +
                "  write: true, " +
                "  writeProperty: 'componentId', " +
                "  weightProperty: 'weight', " +
                "  threshold: 1.0 " +
                "}) " +
                "YIELD nodes AS Nodes, setCount AS NbrOfComponents, writeProperty AS PropertyName";
        String r1 = db.execute(q1).resultAsString();
//        System.out.println(r1);

        String q2 = "MATCH (b:User {name: 'Bridget'}) " +
                    "CREATE (b)-[:LINK {weight: 2.0}]->(new:User {name: 'Mats'})";
        String r2 = db.execute(q2).resultAsString();
//        System.out.println(r2);

        String q3 = "CALL algo.unionFind.stream('User', 'LINK', { " +
                    "  seedProperty: 'componentId', " +
                    "  weightProperty: 'weight', " +
                    "  threshold: 1.0 " +
                    "}) " +
                    "YIELD nodeId, setId " +
                    "RETURN algo.asNode(nodeId).name AS Name, setId AS ComponentId " +
                    "ORDER BY ComponentId, Name";
        String r3 = db.execute(q3).resultAsString();
//        System.out.println(r3);

        String q4 = "CALL algo.unionFind('User', 'LINK', { " +
                    "  seedProperty: 'componentId', " +
                    "  weightProperty: 'weight', " +
                    "  threshold: 1.0, " +
                    "  write: true, " +
                    "  writeProperty: 'componentId' " +
                    "}) " +
                    "YIELD nodes AS Nodes, setCount AS NbrOfComponents, writeProperty AS PropertyName";
        String r4 = db.execute(q4).resultAsString();
//        System.out.println(r4);

        // graph end-state
        System.out.println(db.execute("MATCH (n) RETURN n").resultAsString());
    }

    // Queries from the named graph and Cypher projection example in wcc.adoc
    // used to test that the results are correct in the docs
    @Test
    void namedGraphAndCypherProjection() {
        String q1 = "CALL algo.graph.load('myGraph', 'User', 'LINK');";
        db.execute(q1).resultAsString();

        String q2 = "CALL algo.unionFind.stream(null, null, {graph: 'myGraph'}) " +
                    "YIELD nodeId, setId " +
                    "RETURN algo.asNode(nodeId).name AS Name, setId AS ComponentId " +
                    "ORDER BY ComponentId, Name;";
//        System.out.println(db.execute(q2).resultAsString());

        String q3 = "CALL algo.unionFind.stream( " +
                    "  'MATCH (u:User) RETURN id(u) AS id',  " +
                    "  'MATCH (u1:User)-[:LINK]->(u2:User)  " +
                    "   RETURN id(u1) AS source, id(u2) AS target',  " +
                    "   {graph:'cypher'} " +
                    ") " +
                    "YIELD nodeId, setId " +
                    "RETURN algo.asNode(nodeId).name AS Name, setId AS ComponentId " +
                    "ORDER BY ComponentId, Name";
//        System.out.println(db.execute(q3).resultAsString());
    }
}
