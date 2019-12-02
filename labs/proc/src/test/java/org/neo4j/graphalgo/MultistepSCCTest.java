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
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.multistepscc.MultistepSCC;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 */
class MultistepSCCTest extends ConnectedComponentsTest {

    private static final String DB_CYPHER =
            "CREATE" +
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

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testSequential(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final MultistepSCC multistep = new MultistepSCC(graph, Pools.DEFAULT, 1, 0);
        multistep.compute();

        assertCC(multistep.getConnectedComponents());

        assertEquals(3, multistep.getMaxSetSize());
        assertEquals(3, multistep.getMinSetSize());
        assertEquals(3, multistep.getSetCount());

    }

    @AllGraphTypesWithoutCypherTest
    void testParallel(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final MultistepSCC multistep = new MultistepSCC(graph, Pools.DEFAULT, 4, 0);
        multistep.compute();

        assertCC(multistep.getConnectedComponents());

        assertEquals(3, multistep.getMaxSetSize());
        assertEquals(3, multistep.getMinSetSize());
        assertEquals(3, multistep.getSetCount());
    }

    @AllGraphTypesWithoutCypherTest
    void testHighCut(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final MultistepSCC multistep = new MultistepSCC(graph, Pools.DEFAULT, 4, 100_000);
        multistep.compute();

        assertCC(multistep.getConnectedComponents());

        assertEquals(3, multistep.getMaxSetSize());
        assertEquals(3, multistep.getMinSetSize());
        assertEquals(3, multistep.getSetCount());
    }

    private void setup(Class<? extends GraphFactory> graphImpl) {
        graph = new GraphLoader(db)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .withRelationshipProperties(PropertyMapping.of("cost", Double.MAX_VALUE))
                .load(graphImpl);
    }
}
