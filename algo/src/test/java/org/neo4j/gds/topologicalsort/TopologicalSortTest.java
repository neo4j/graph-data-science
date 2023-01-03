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
package org.neo4j.gds.topologicalsort;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class TopologicalSortTest {

    private static TopologicalSortConfig CONFIG = new TopologicalSortConfigImpl.Builder().build();

    @GdlGraph(graphNamePrefix = "basic")
    private static final String basicQuery =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n0)-->(n1)" +
        ", (n0)-->(n2)" +
        ", (n2)-->(n1)" +
        ", (n3)-->(n0)";

    @Inject
    private TestGraph basicGraph;

    @Test
    void shouldSortRight() {
        TopologicalSort ts = new TopologicalSort(basicGraph, CONFIG, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.value();

        long first = nodes.get(0);
        long second = nodes.get(1);
        long third = nodes.get(2);
        long fourth = nodes.get(3);
        assertEquals(4, result.size());
        assertEquals(3, first);
        assertEquals(0, second);
        assertEquals(2, third);
        assertEquals(1, fourth);
    }

    @GdlGraph(graphNamePrefix = "allCycle")
    private static final String allCycleQuery =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n0)-->(n1)" +
        ", (n1)-->(n3)" +
        ", (n2)-->(n0)" +
        ", (n3)-->(n2)";

    @Inject
    private TestGraph allCycleGraph;

    @Test
    void allCycleShouldGiveEmptySorting() {
        TopologicalSort ts = new TopologicalSort(allCycleGraph, CONFIG, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.value();

        assertEquals(0, result.size());
    }

    @GdlGraph(graphNamePrefix = "selfLoop")
    private static final String selfLoopQuery =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n0)-->(n1)" +
        ", (n1)-->(n1)" +
        ", (n2)-->(n2)";

    @Inject
    private TestGraph selfLoopGraph;

    @Test
    void ShouldExcludeSelfLoops() {
        TopologicalSort ts = new TopologicalSort(selfLoopGraph, CONFIG, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.value();

        long first = nodes.get(0);
        assertEquals(1, result.size());
        assertEquals(0, first);
    }

    @GdlGraph(graphNamePrefix = "last", idOffset = 20)
    private static final String lastQuery =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n4)" +
        ", (n5)" +
        ", (n6)" +
        ", (n7)" +
        ", (n8)" +
        ", (n9)" +
        ", (n20)" +
        ", (n21)" +
        ", (n22)" +
        ", (n23)" +
        ", (n24)" +
        ", (n25)" +
        ", (n26)" +
        ", (n27)" +
        ", (n28)" +
        ", (n29)" +
        ", (n100)" +
        ", (n0)-->(n20)" +
        ", (n1)-->(n21)" +
        ", (n2)-->(n22)" +
        ", (n3)-->(n23)" +
        ", (n4)-->(n24)" +
        ", (n5)-->(n25)" +
        ", (n6)-->(n26)" +
        ", (n7)-->(n27)" +
        ", (n8)-->(n28)" +
        ", (n9)-->(n29)" +
        ", (n0)-->(n100)" +
        ", (n1)-->(n100)" +
        ", (n2)-->(n100)" +
        ", (n3)-->(n100)" +
        ", (n4)-->(n100)" +
        ", (n5)-->(n100)" +
        ", (n6)-->(n100)" +
        ", (n7)-->(n100)" +
        ", (n8)-->(n100)" +
        ", (n9)-->(n100)" +
        ", (n20)-->(n100)" +
        ", (n21)-->(n100)" +
        ", (n22)-->(n100)" +
        ", (n23)-->(n100)" +
        ", (n24)-->(n100)" +
        ", (n25)-->(n100)" +
        ", (n26)-->(n100)" +
        ", (n27)-->(n100)" +
        ", (n28)-->(n100)" +
        ", (n29)-->(n100)";

    @Inject
    private TestGraph lastGraph;

    @Test
    void hundredShouldComeLast() {

        // Despite all nodes have relations to node 100 (therefore it is in "risk" of being handled not in order), this node must be the last in sorting
        long nodeCount = lastGraph.nodeCount();
        TopologicalSort ts = new TopologicalSort(lastGraph, CONFIG, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.value();

        assertEquals(nodeCount, result.size());
        long last = nodes.get(nodeCount - 1);
        assertEquals(lastGraph.toMappedNodeId("n100"), last);
    }

    @GdlGraph(graphNamePrefix = "cycles")
    private static final String cyclesQuery =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n4)" +
        ", (n5)" +
        ", (n6)" +
        ", (n7)" +
        ", (n8)" +
        ", (n0)-->(n1)" +
        ", (n0)-->(n2)" +
        ", (n0)-->(n5)" +
        ", (n1)-->(n0)" +
        ", (n1)-->(n5)" +
        ", (n2)-->(n3)" +
        ", (n3)-->(n4)" +
        ", (n4)-->(n3)" +
        ", (n5)-->(n0)" +
        ", (n5)-->(n1)" +
        ", (n6)-->(n1)" +
        ", (n7)-->(n6)" +
        ", (n8)-->(n6)";

    @Inject
    private TestGraph cyclesGraph;

    @Test
    void shouldNotIncludeCycles() {
        TopologicalSort ts = new TopologicalSort(cyclesGraph, CONFIG, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.value();

        long first = nodes.get(0);
        long second = nodes.get(1);
        long third = nodes.get(2);
        assertTrue(first == 7 || first == 8);
        assertTrue(second == 7 || second == 8);
        assertEquals(3, result.size());
        assertEquals(6, third);
    }
}
