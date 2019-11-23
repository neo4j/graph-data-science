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

import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.scc.SCCIterativeTarjan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 */
class IterativeTarjanSCCTest extends ConnectedComponentsTest {

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
    void setupGraphDb() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(StronglyConnectedComponentsProc.class);
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void shutdownGraph() {
        db.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testDirect() {

        final SCCIterativeTarjan tarjan = new SCCIterativeTarjan(graph, AllocationTracker.EMPTY)
                .compute();

        assertCC(tarjan.getConnectedComponents());
        assertEquals(3, tarjan.getMaxSetSize());
        assertEquals(3, tarjan.getMinSetSize());
        assertEquals(3, tarjan.getSetCount());
    }

    @AllGraphTypesWithoutCypherTest
    void testCypher(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        String cypher = "CALL algo.scc.iterative('', '', {write:true}) YIELD loadMillis, computeMillis, writeMillis";

        db.execute(cypher).accept(row -> {
            long loadMillis = row.getNumber("loadMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();
            assertNotEquals(-1, loadMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
            return true;
        });

        String cypher2 = "MATCH (n) RETURN n.partition as c";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        db.execute(cypher2).accept(row -> {
            testMap.addTo(row.getNumber("c").intValue(), 1);
            return true;
        });

        // 3 sets with 3 elements each
        assertEquals(3, testMap.size());
        for (IntIntCursor cursor : testMap) {
            assertEquals(3, cursor.value);
        }
    }

    @AllGraphTypesWithoutCypherTest
    void testCypherStream(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final IntIntScatterMap testMap = new IntIntScatterMap();

        String cypher = "CALL algo.scc.iterative.stream() YIELD nodeId, partition";

        db.execute(cypher).accept(row -> {
            testMap.addTo(row.getNumber("partition").intValue(), 1);
            return true;
        });

        // 3 sets with 3 elements each
        assertEquals(3, testMap.size());
        for (IntIntCursor cursor : testMap) {
            assertEquals(3, cursor.value);
        }
    }

    private void setup(Class<? extends GraphFactory> graphFactory) {
        graph = new GraphLoader(db)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .withRelationshipProperties(PropertyMapping.of("cost", Double.MAX_VALUE))
                .load(graphFactory);
    }
}
