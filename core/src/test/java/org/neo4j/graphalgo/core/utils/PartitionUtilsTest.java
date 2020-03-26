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
package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionUtilsTest {

    public class TestTask implements Runnable {

        public final long start;
        public final long nodeCount;

        TestTask(long start, long nodeCount) {
            this.start = start;
            this.nodeCount = nodeCount;
        }

        @Override
        public void run() {

        }

        @Override
        public String toString() {
            return String.format("(%d, %d)", start, nodeCount);
        }
    }

    @Test
    void testAlignment() {
        long alignTo = 64;
        long nodeCount = 200;
        int concurrency = 2;

        List<Partition> partitions = PartitionUtils.numberAlignedPartitioning(
            concurrency,
            nodeCount,
            alignTo
        );

        List<TestTask> tasks = partitions
            .stream()
            .map(partition -> new TestTask(partition.startNode, partition.nodeCount))
            .collect(Collectors.toList());

        assertEquals(2, tasks.size());
        assertTrue(
            tasks.stream().anyMatch((t) -> t.start == 0 && t.nodeCount == 128),
            String.format("Expected task with start %d and nodeCount %d, but found %s", 0, 128, tasks)
        );
        assertTrue(
            tasks.stream().anyMatch((t) -> t.start == 128 && t.nodeCount == 72),
            String.format("Expected task with start %d and nodeCount %d, but found %s", 128, 72, tasks)
        );
    }

    @Test
    void testDegreePartitioning() {
        Graph graph = TestGraph.Builder.fromGdl(
            "(a)-->(b)" +
            "(a)-->(c)" +
            "(b)-->(a)" +
            "(b)-->(c)"
        );

        List<Partition> partitions = PartitionUtils.degreePartition(graph, 2);
        assertEquals(2, partitions.size());
        assertEquals(0, partitions.get(0).startNode);
        assertEquals(2, partitions.get(0).nodeCount);
        assertEquals(2, partitions.get(1).startNode);
        assertEquals(1, partitions.get(1).nodeCount);
    }

    @Test
    void testDegreePartitioningWithNodeFilter() {
        Graph graph = TestGraph.Builder.fromGdl(
            "(a)-->(b)" +
            "(a)-->(c)" +
            "(b)-->(a)" +
            "(b)-->(c)"
        );
        BitSet nodeFilter = new BitSet(graph.nodeCount());
        nodeFilter.set(0);
        nodeFilter.set(2);

        List<Partition> partitions = PartitionUtils.degreePartition(
            new SetBitsIterable(nodeFilter).primitiveLongIterator(), graph, 2
        );
        assertEquals(1, partitions.size());
        assertEquals(0, partitions.get(0).startNode);
        assertEquals(3, partitions.get(0).nodeCount);
    }

}