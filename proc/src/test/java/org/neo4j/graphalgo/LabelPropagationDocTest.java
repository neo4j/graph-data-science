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

            "CREATE (nAlice)-[:FOLLOW]->(nBridget)" +
            "CREATE (nAlice)-[:FOLLOW]->(nCharles)" +
            "CREATE (nMark)-[:FOLLOW]->(nDoug)" +
            "CREATE (nBridget)-[:FOLLOW]->(nMichael)" +
            "CREATE (nDoug)-[:FOLLOW]->(nMark)" +
            "CREATE (nMichael)-[:FOLLOW]->(nAlice)" +
            "CREATE (nAlice)-[:FOLLOW]->(nMichael)" +
            "CREATE (nBridget)-[:FOLLOW]->(nAlice)" +
            "CREATE (nMichael)-[:FOLLOW]->(nBridget)" +
            "CREATE (nCharles)-[:FOLLOW]->(nDoug);";

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
                    "YIELD nodeId, label " +
                    "RETURN algo.asNode(nodeId).name AS Name, label AS CommunityId " +
                    "ORDER BY CommunityId, Name";
        System.out.println(db.execute(q1).resultAsString());
        
        String q2 = "CALL algo.labelPropagation('User', 'FOLLOW', {" +
                    "  direction: 'OUTGOING'," +
                    "  iterations: 10," +
                    "  writeProperty: 'community'," +
                    "  write: true" +
                    "})" +
                    "YIELD nodes, iterations, communityCount, writeProperty;";
        System.out.println(db.execute(q2).resultAsString());
    }

    @Test
    void seeded(){
        String q1 = "CALL algo.labelPropagation.stream('User', 'FOLLOW', {" +
                    "   iterations: 10," +
                    "   seedProperty: 'seed_label'," +
                    "   direction: 'OUTGOING'" +
                    "})" +
                    "YIELD nodeId, label " +
                    "RETURN algo.asNode(nodeId).name AS Name, label AS CommunityId " +
                    "ORDER BY CommunityId, Name";
        System.out.println(db.execute(q1).resultAsString());
    }

    // Queries from the named graph and Cypher projection example in label-propagation.adoc
    // used to test that the results are correct in the docs
    @Test
    void namedGraphAndCypherProjection() {
        String loadGraph = "CALL algo.graph.load('myGraph', 'User', 'FOLLOW');";
        db.execute(loadGraph);

        String q1 = "CALL algo.labelPropagation.stream(null, null, {" +
                    "   graph: 'myGraph'," +
                    "   direction: 'OUTGOING'," +
                    "   iterations: 10" +
                    "})" +
                    "YIELD nodeId, label " +
                    "RETURN algo.asNode(nodeId).name AS Name, label AS ComponentId " +
                    "ORDER BY ComponentId, Name";
        String r1 = db.execute(q1).resultAsString();
        System.out.println(r1);

        String q2 = "CALL algo.labelPropagation.stream(" +
                    "  'MATCH (p:User) RETURN id(p) AS id, p.weight AS weight, id(p) AS value'," +
                    "  'MATCH (p1:User)-[f:FOLLOW]->(p2:User)" +
                    "   RETURN id(p1) AS source, id(p2) AS target, f.weight AS weight',{" +
                    "   iterations: 10," +
                    "   graph: 'cypher'})" +
                    "YIELD nodeId, label " +
                    "RETURN algo.asNode(nodeId).name AS Name, label AS ComponentId " +
                    "ORDER BY ComponentId, Name;";
        String r2 = db.execute(q2).resultAsString();
        System.out.println(r2);
        assertEquals(r1,r2);
    }
}
