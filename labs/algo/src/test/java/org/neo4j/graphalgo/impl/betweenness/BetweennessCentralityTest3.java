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
package org.neo4j.graphalgo.impl.betweenness;

import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.Direction;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;

/**
 * (B)-->(C)-->(D)
 *  ↑     |     |
 *  |     |     |
 *  |     ↓     ↓
 * (A)-->(F)<--(E)
 */
class BetweennessCentralityTest3 extends AlgoTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (f:Node {name: 'f'})" +
            ", (a)-[:TYPE]->(b)" +
            ", (a)-[:TYPE]->(f)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(d)" +
            ", (c)-[:TYPE]->(f)" +
            ", (e)-[:TYPE]->(f)" +
            ", (d)-[:TYPE]->(e)";

    private static Graph graph;

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
    }

    @BeforeEach
    void shutdownGraphDb() {
        if (db != null) db.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testBetweennessCentralityOutgoing(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        Map<String, Double> actual = new BetweennessCentrality(graph)
                .withDirection(Direction.OUTGOING)
                .compute()
                .resultStream()
                .collect(Collectors.toMap(r -> name(r.nodeId), r -> r.centrality));

        Map<String, Double> expected = new HashMap<>();
        expected.put("a", 0.0);
        expected.put("b", 3.0);
        expected.put("c", 5.0);
        expected.put("d", 3.0);
        expected.put("e", 1.0);
        expected.put("f", 0.0);

        assertThat(actual.keySet(), equalTo(expected.keySet()));
        expected.forEach((key, value) -> assertThat(actual.get(key), closeTo(value, 1e-14)));
    }

    @AllGraphTypesWithoutCypherTest
    void testBetweennessCentralityIncoming(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        Map<String, Double> actual = new BetweennessCentrality(graph)
                .withDirection(Direction.INCOMING)
                .compute()
                .resultStream()
                .collect(Collectors.toMap(r -> name(r.nodeId), r -> r.centrality));

        Map<String, Double> expected = new HashMap<>();
        expected.put("a", 0.0);
        expected.put("b", 3.0);
        expected.put("c", 5.0);
        expected.put("d", 3.0);
        expected.put("e", 1.0);
        expected.put("f", 0.0);

        assertThat(actual.keySet(), equalTo(expected.keySet()));
        expected.forEach((key, value) -> assertThat(actual.get(key), closeTo(value, 1e-14)));
    }

    @AllGraphTypesWithoutCypherTest
    void testBetweennessCentralityBoth(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        Map<String, Double> actual = new BetweennessCentrality(graph)
                .withDirection(Direction.BOTH)
                .compute()
                .resultStream()
                .collect(Collectors.toMap(r -> name(r.nodeId), r -> r.centrality));

        Map<String, Double> expected = new HashMap<>();
        expected.put("a", 5.0 / 6.0);
        expected.put("b", 5.0 / 6.0);
        expected.put("c", 10.0 / 3.0);
        expected.put("d", 5.0 / 6.0);
        expected.put("e", 5.0 / 6.0);
        expected.put("f", 10.0 / 3.0);

        assertThat(actual.keySet(), equalTo(expected.keySet()));
        expected.forEach((key, value) -> assertThat(actual.get(key), closeTo(value, 1e-14)));
    }

    private void setup(Class<? extends GraphFactory> graphFactory) {
        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .load(graphFactory);
    }

    private String name(long id) {
        String[] name = {""};
        runQuery(
            "MATCH (n:Node) WHERE id(n) = " + id + " RETURN n.name as name",
            row -> name[0] = row.getString("name")
        );
        return name[0];
    }

}
