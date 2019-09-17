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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.graphalgo.ConnectedComponentsTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.scc.SCCIterativeTarjan;
import org.neo4j.graphalgo.impl.scc.SCCTunedTarjan;

import java.util.Arrays;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 */
class SCCTest extends ConnectedComponentsTest {

    private static final String DB_CYPHER = "CREATE" +
                                            "  (a:Node {name: 'a'})" +
                                            ", (b:Node {name: 'b'})" +
                                            ", (c:Node {name: 'c'})" +
                                            ", (d:Node {name: 'd'})" +
                                            ", (e:Node {name: 'e'})" +
                                            ", (f:Node {name: 'f'})" +
                                            ", (g:Node {name: 'g'})" +
                                            ", (h:Node {name: 'h'})" +
                                            ", (i:Node {name: 'i'})" +
                                            ", (x:Node {name: 'x'})" +

                                            ", (a)-[:TYPE {cost: 5}]->(b)" +
                                            ", (b)-[:TYPE {cost: 5}]->(c)" +
                                            ", (c)-[:TYPE {cost: 5}]->(a)" +

                                            ", (d)-[:TYPE {cost: 2}]->(e)" +
                                            ", (e)-[:TYPE {cost: 2}]->(f)" +
                                            ", (f)-[:TYPE {cost: 2}]->(d)" +

                                            ", (a)-[:TYPE {cost: 2}]->(d)" +

                                            ", (g)-[:TYPE {cost: 3}]->(h)" +
                                            ", (h)-[:TYPE {cost: 3}]->(i)" +
                                            ", (i)-[:TYPE {cost: 3}]->(g)";

    @BeforeAll
    static void setupGraphDb() {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.execute(DB_CYPHER);
    }

    @AfterAll
    static void shutdown() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testHugeIterativeScc(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertCC(new SCCIterativeTarjan(graph, AllocationTracker.EMPTY)
                .compute()
                .getConnectedComponents());
    }

    @AllGraphTypesWithoutCypherTest
    void testTunedTarjan(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        int[] connectedComponents = new SCCTunedTarjan(graph)
                .compute()
                .getConnectedComponents();
        HugeLongArray longs = HugeLongArray.of(Arrays.stream(connectedComponents).mapToLong(i -> (long) i).toArray());
        assertCC(longs);
    }

    private void setup(Class<? extends GraphFactory> graphImpl) {
        graph = new GraphLoader(DB)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(graphImpl);
    }
}
