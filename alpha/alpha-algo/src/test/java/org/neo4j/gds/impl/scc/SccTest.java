/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.impl.scc;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@GdlExtension
class SccTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +

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

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testDirect() {
        SccAlgorithm scc = new SccAlgorithm(graph, AllocationTracker.empty());
        HugeLongArray components = scc.compute();

        assertCC(components);
        assertEquals(3, scc.getMaxSetSize());
        assertEquals(3, scc.getMinSetSize());
        assertEquals(3, scc.getSetCount());
    }

    @Test
    void testHugeIterativeScc() {
        SccAlgorithm algo = new SccAlgorithm(graph, AllocationTracker.empty());
        HugeLongArray components = algo.compute();
        assertCC(components);
    }

    private void assertCC(HugeLongArray connectedComponents) {
        assertBelongSameSet(connectedComponents,
            idFunction.of("a"),
            idFunction.of("b"),
            idFunction.of("c")
        );
        assertBelongSameSet(connectedComponents,
            idFunction.of("d"),
            idFunction.of("e"),
            idFunction.of("f")
        );
        assertBelongSameSet(connectedComponents,
            idFunction.of("g"),
            idFunction.of("h"),
            idFunction.of("i")
        );
    }

    // TODO: Try to get this working with AssertJ
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

}
