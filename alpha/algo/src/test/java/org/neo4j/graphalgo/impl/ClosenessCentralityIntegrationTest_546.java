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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality;

class ClosenessCentralityIntegrationTest_546 extends AlgoTestBase {

    private String name(long id) {
        String[] name = {""};
        runQuery("MATCH (n) WHERE id(n) = " + id + " RETURN n.id as name", row -> name[0] = row.getString("name"));
        if (name[0].isEmpty()) {
            throw new IllegalArgumentException("unknown id " + id);
        }
        return name[0];
    }

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @Test
    void test547() {

        String importQuery =
                "CREATE (alice:Person{id:\"Alice\"}),\n" +
                "       (michael:Person{id:\"Michael\"}),\n" +
                "       (karin:Person{id:\"Karin\"}),\n" +
                "       (chris:Person{id:\"Chris\"}),\n" +
                "       (will:Person{id:\"Will\"}),\n" +
                "       (mark:Person{id:\"Mark\"})\n" +
                "CREATE (michael)<-[:KNOWS]-(karin),\n" +
                "       (michael)-[:KNOWS]->(chris),\n" +
                "       (will)-[:KNOWS]->(michael),\n" +
                "       (mark)<-[:KNOWS]-(michael),\n" +
                "       (mark)-[:KNOWS]->(will),\n" +
                "       (alice)-[:KNOWS]->(michael),\n" +
                "       (will)-[:KNOWS]->(chris),\n" +
                "       (chris)-[:KNOWS]->(karin);";

        runQuery(importQuery);

        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .globalProjection(Projection.UNDIRECTED)
            .build()
            .load(HugeGraphFactory.class);

        System.out.println("547:");
        MSClosenessCentrality algo = new MSClosenessCentrality(
            graph,
            AllocationTracker.EMPTY,
            2,
            Pools.DEFAULT,
            false
        );
        algo.compute();
        algo.resultStream()
            .forEach(this::print);

    }

    @Test
    void test546() {

        String importQuery =
                "CREATE (nAlice:User {id:'Alice'})\n" +
                        ",(nBridget:User {id:'Bridget'})\n" +
                        ",(nCharles:User {id:'Charles'})\n" +
                        ",(nMark:User {id:'Mark'})\n" +
                        ",(nMichael:User {id:'Michael'})\n" +
                        "CREATE (nAlice)-[:FRIEND]->(nBridget)\n" +
                        ",(nAlice)<-[:FRIEND]-(nBridget)\n" +
                        ",(nAlice)-[:FRIEND]->(nCharles)\n" +
                        ",(nAlice)<-[:FRIEND]-(nCharles)\n" +
                        ",(nMark)-[:FRIEND]->(nMichael)\n" +
                        ",(nMark)<-[:FRIEND]-(nMichael);";

        runQuery(importQuery);

        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .globalProjection(Projection.UNDIRECTED)
            .build()
            .load(HugeGraphFactory.class);

        System.out.println("546:");
        MSClosenessCentrality algo = new MSClosenessCentrality(
            graph,
            AllocationTracker.EMPTY,
            2,
            Pools.DEFAULT,
            false
        );
        algo.compute();
        algo.resultStream()
            .forEach(this::print);

    }

    private void print(MSClosenessCentrality.Result result) {
        System.out.printf("%s | %.3f%n", name(result.nodeId), result.centrality);
    }
}
