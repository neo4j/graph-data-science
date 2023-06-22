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
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class TopologicalSortTest {
    private static TopologicalSortBaseConfig CONFIG = new TopologicalSortStreamConfigImpl.Builder().concurrency(4)
        .computeLongestPathDistances(true)
        .build();
    private static TopologicalSortBaseConfig BASIC_CONFIG = new TopologicalSortStreamConfigImpl.Builder().concurrency(4)
        .build();

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
        TopologicalSort ts = new TopologicalSort(basicGraph, CONFIG, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.sortedNodes();

        long first = nodes.get(0);
        long second = nodes.get(1);
        long third = nodes.get(2);
        long fourth = nodes.get(3);
        assertEquals(4, result.size());
        assertEquals(3, first);
        assertEquals(0, second);
        assertEquals(2, third);
        assertEquals(1, fourth);

        var longestPathsDistances = result.longestPathDistances().get();
        var firstLongestPathDistance = longestPathsDistances.get(0);
        var secondLongestPathDistance = longestPathsDistances.get(1);
        var thirdLongestPathDistance = longestPathsDistances.get(2);
        var fourthLongestPathDistance = longestPathsDistances.get(3);

        assertEquals(1, firstLongestPathDistance);
        assertEquals(3, secondLongestPathDistance);
        assertEquals(2, thirdLongestPathDistance);
        assertEquals(0, fourthLongestPathDistance);
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
        TopologicalSort ts = new TopologicalSort(allCycleGraph, BASIC_CONFIG, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.sortedNodes();

        assertEquals(0, result.size());
    }

    @Test
    void shouldNotAllocateArraysOnBasicConfig() {
        TopologicalSort ts = new TopologicalSort(allCycleGraph, BASIC_CONFIG, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();

        assertTrue(result.longestPathDistances().isEmpty());
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
        TopologicalSort ts = new TopologicalSort(selfLoopGraph, CONFIG, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.sortedNodes();
        var longestPathsDistances = result.longestPathDistances().get();

        long first = nodes.get(0);
        assertEquals(1, result.size());
        assertEquals(0, first);

        // Note: paths of ignored nodes are not calculated, and can have any value (unimportant implementation detail)
        var firstLongestPathDistance = longestPathsDistances.get(0);
        assertEquals(0, firstLongestPathDistance);
    }

    // This graph looks like an asterisk. nodes 0-9 are at the outermost layer, they point to nodes 20-29 in the middle
    // layer, and everyone points to node 100 in the center.
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
        TopologicalSort ts = new TopologicalSort(lastGraph, CONFIG, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.sortedNodes();
        var longestPathsDistances = result.longestPathDistances().get();

        assertEquals(nodeCount, result.size());
        long last = nodes.get(nodeCount - 1);
        assertEquals(lastGraph.toMappedNodeId("n100"), last);

        var n0distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n0"));
        var n1distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n1"));
        var n2distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n2"));
        var n3distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n3"));
        var n4distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n4"));
        var n5distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n5"));
        var n6distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n6"));
        var n7distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n7"));
        var n8distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n8"));
        var n9distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n9"));
        var n20distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n20"));
        var n21distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n21"));
        var n22distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n22"));
        var n23distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n23"));
        var n24distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n24"));
        var n25distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n25"));
        var n26distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n26"));
        var n27distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n27"));
        var n28distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n28"));
        var n29distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n29"));
        var n100distance = longestPathsDistances.get(lastGraph.toMappedNodeId("n100"));

        assertEquals(0, n0distance);
        assertEquals(0, n1distance);
        assertEquals(0, n2distance);
        assertEquals(0, n3distance);
        assertEquals(0, n4distance);
        assertEquals(0, n5distance);
        assertEquals(0, n6distance);
        assertEquals(0, n7distance);
        assertEquals(0, n8distance);
        assertEquals(0, n9distance);
        assertEquals(1, n20distance);
        assertEquals(1, n21distance);
        assertEquals(1, n22distance);
        assertEquals(1, n23distance);
        assertEquals(1, n24distance);
        assertEquals(1, n25distance);
        assertEquals(1, n26distance);
        assertEquals(1, n27distance);
        assertEquals(1, n28distance);
        assertEquals(1, n29distance);
        assertEquals(2, n100distance);
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
        TopologicalSort ts = new TopologicalSort(cyclesGraph, CONFIG, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        HugeLongArray nodes = result.sortedNodes();
        var longestPathsDistances = result.longestPathDistances().get();

        long first = nodes.get(0);
        long second = nodes.get(1);
        long third = nodes.get(2);

        assertThat(List.of(first, second)).containsExactlyInAnyOrder(
            cyclesGraph.toMappedNodeId("n7"),
            cyclesGraph.toMappedNodeId("n8")
        );
        assertEquals(3, result.size());
        assertThat(third).isEqualTo(cyclesGraph.toMappedNodeId("n6"));

        var n6distance = longestPathsDistances.get(cyclesGraph.toMappedNodeId("n6"));
        var n7distance = longestPathsDistances.get(cyclesGraph.toMappedNodeId("n7"));
        var n8distance = longestPathsDistances.get(cyclesGraph.toMappedNodeId("n8"));

        assertEquals(1, n6distance);
        assertEquals(0, n7distance);
        assertEquals(0, n8distance);
    }

    @Test
    void randomShouldContainAllNodesOnDag() {
        // this is also testing the RGG forceDag flag, as running topo sort is the best way to test it
        Random rand = new Random();
        var graph = RandomGraphGenerator.builder()
            .nodeCount(100)
            .averageDegree(6)
            .relationshipDistribution(RelationshipDistribution.RANDOM)
            .seed(rand.nextInt())
            .forceDag(true)
            .build()
            .generate();

        TopologicalSort ts = new TopologicalSort(graph, BASIC_CONFIG, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        assertEquals(100, result.size());
    }

    @Test
    void stableShouldContainAllNodesOnDag() {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(1000_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.RANDOM)
            .seed(42)
            .forceDag(true)
            .build()
            .generate();

        TopologicalSort ts = new TopologicalSortFactory<>().build(graph, BASIC_CONFIG, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();
        assertEquals(1000_000, result.size());
    }

    @Test
    void shouldLogProgress() {
        var tsFactory = new TopologicalSortFactory<>();
        var progressTask = tsFactory.progressTask(lastGraph, BASIC_CONFIG);
        var log = Neo4jProxy.testLog();
        var testTracker = new TestProgressTracker(
            progressTask,
            log,
            BASIC_CONFIG.concurrency(),
            EmptyTaskRegistryFactory.INSTANCE
        );

        TopologicalSort ts = tsFactory.build(lastGraph, BASIC_CONFIG, testTracker);
        ts.compute();

        String taskName = tsFactory.taskName();

        assertTrue(log.containsMessage(TestLog.INFO, taskName + " :: Start"));
        assertTrue(log.containsMessage(TestLog.INFO, taskName + " :: Finished"));
    }
}
