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
package org.neo4j.graphalgo;

import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.scc.SCCAlgorithm;
import org.neo4j.graphdb.Node;

import java.util.Arrays;
import java.util.List;

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
class StronglyConnectedComponentsTest extends BaseProcTest {

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

    private Graph graph;

    @BeforeEach
    void setupGraphDb() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(StronglyConnectedComponentsProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void shutdownGraph() {
        db.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testDirect(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        SCCAlgorithm scc = new SCCAlgorithm(graph, AllocationTracker.EMPTY);
        scc.compute();

        assertCC(scc.getConnectedComponents());
        assertEquals(3, scc.getMaxSetSize());
        assertEquals(3, scc.getMinSetSize());
        assertEquals(3, scc.getSetCount());
    }

    @AllGraphTypesWithoutCypherTest
    void testHugeIterativeScc(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        SCCAlgorithm algo = new SCCAlgorithm(graph, AllocationTracker.EMPTY);
        algo.compute();
        assertCC(algo.getConnectedComponents());
    }

    @AllGraphTypesWithoutCypherTest
    void testCypher(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        String cypher = "CALL algo.scc('', '', {write:true}) YIELD loadMillis, computeMillis, writeMillis";

        runQuery(cypher, row -> {
            long loadMillis = row.getNumber("loadMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();
            assertNotEquals(-1, loadMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
        });

        String cypher2 = "MATCH (n) RETURN n.partition as c";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        runQuery(cypher2, row -> testMap.addTo(row.getNumber("c").intValue(), 1));

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

        String cypher = "CALL algo.scc.stream() YIELD nodeId, partition";

        runQuery(cypher, row -> testMap.addTo(row.getNumber("partition").intValue(), 1));

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

    private void assertBelongSameSet(HugeLongArray data, Long... expected) {
        // check if all belong to same set
        final long needle = data.get(expected[0]);
        for (long l : expected) {
            assertEquals(needle, data.get(l));
        }

        final List<Long> exp = Arrays.asList(expected);
        // check no other element belongs to this set
        for (long i = 0; i < data.size(); i++) {
            if (exp.contains(i)) {
                continue;
            }
            assertNotEquals(needle, data.get(i));
        }
    }

    private long getMappedNodeId(String name) {
        final Node[] node = new Node[1];
        runQuery("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n", row -> {
            node[0] = row.getNode("n");
        });
        return graph.toMappedNodeId(node[0].getId());
    }

    private void assertCC(HugeLongArray connectedComponents) {
        assertBelongSameSet(connectedComponents,
            getMappedNodeId("a"),
            getMappedNodeId("b"),
            getMappedNodeId("c"));
        assertBelongSameSet(connectedComponents,
            getMappedNodeId("d"),
            getMappedNodeId("e"),
            getMappedNodeId("f"));
        assertBelongSameSet(connectedComponents,
            getMappedNodeId("g"),
            getMappedNodeId("h"),
            getMappedNodeId("i"));
    }
}
