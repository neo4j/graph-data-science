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
package org.neo4j.graphalgo.core.huge;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class HugeGraphUnweightedTest {

    private static final int BATCH_SIZE = 100;
    public static final RelationshipType TYPE = RelationshipType.withName("TYPE");

    private GraphDatabaseAPI db;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute("CREATE (a:Node {name:'a'})\n" +
                   "CREATE (b:Node {name:'b'})\n" +
                   "CREATE (c:Node {name:'c'})\n" +
                   "CREATE (d:Node2 {name:'d'})\n" +
                   "CREATE (e:Node2 {name:'e'})\n" +

                   "CREATE" +
                   " (a)-[:TYPE {prop:1}]->(b),\n" +
                   " (e)-[:TYPE {prop:2}]->(d),\n" +
                   " (d)-[:TYPE {prop:3}]->(c),\n" +
                   " (a)-[:TYPE {prop:4}]->(c),\n" +
                   " (a)-[:TYPE {prop:5}]->(d),\n" +
                   " (a)-[:TYPE2 {prop:6}]->(d),\n" +
                   " (b)-[:TYPE2 {prop:7}]->(e),\n" +
                   " (a)-[:TYPE2 {prop:8}]->(e)");
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @ParameterizedTest
    @EnumSource(Direction.class)
    void relationshipIteratorShouldReturnFallbackWeight(Direction direction) {
        Graph graph = loadGraph(db, direction);

        double fallbackWeight = 42D;
        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, direction, fallbackWeight, (s, t, w) -> {
                assertEquals(fallbackWeight, w);
                return true;
            });
            return true;
        });
    }

    @ParameterizedTest
    @EnumSource(Direction.class)
    void weightOfShouldReturnFallbackWeight(Direction direction) {
        Graph graph = loadGraph(db, direction);

        double fallbackWeight = 42D;
        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, direction, (s, t) -> {
                assertEquals(fallbackWeight, graph.weightOf(s, t, fallbackWeight));
                return true;
            });
            return true;
        });
    }

    private Graph loadGraph(final GraphDatabaseAPI db, Direction direction) {
        return new GraphLoader(db)
                .withDirection(direction)
                .withExecutorService(Pools.DEFAULT)
                .withBatchSize(BATCH_SIZE)
                .load(HugeGraphFactory.class);
    }

}
