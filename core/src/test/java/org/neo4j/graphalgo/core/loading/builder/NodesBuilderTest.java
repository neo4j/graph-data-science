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
package org.neo4j.graphalgo.core.loading.builder;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.loading.factory.GraphFactory;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.HashSet;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NodesBuilderTest {

    @Test
    void parallelIdMapBuilder() {
        long nodeCount = 100;
        int concurrency = 4;
        var idMapBuilder = GraphFactory.nodesBuilder(nodeCount, false, concurrency, AllocationTracker.empty());

        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, nodeCount),
            concurrency,
            stream -> stream.forEach(idMapBuilder::addNode)
        );

        var idMap = idMapBuilder.build();

        assertEquals(nodeCount, idMap.nodeCount());
    }

    @Test
    void parallelIdMapBuilderWithDuplicateNodes() {
        long attempts = 100;
        int concurrency = 4;
        var idMapBuilder = GraphFactory.nodesBuilder(attempts, false, concurrency, AllocationTracker.empty());

        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, attempts),
            concurrency,
            stream -> stream.forEach(originalId -> idMapBuilder.addNode(0))
        );

        var idMap = idMapBuilder.build();

        assertEquals(1, idMap.nodeCount());
    }

    @Test
    void parallelIdMapBuilderWithLabels() {
        long attempts = 100;
        int concurrency = 4;
        var labels1 = new HashSet<>(NodeLabel.listOf("Label1"));
        var labels2 = new HashSet<>(NodeLabel.listOf("Label2"));

        var idMapBuilder = GraphFactory.nodesBuilder(attempts, true, concurrency, AllocationTracker.empty());

        ParallelUtil.parallelStreamConsume(LongStream.range(0, attempts), concurrency, stream -> stream.forEach(
            originalId -> {
                var labels = originalId % 2 == 0
                    ? labels1.toArray(NodeLabel[]::new)
                    : labels2.toArray(NodeLabel[]::new);

                idMapBuilder.addNode(originalId, labels);
            })
        );

        var idMap = idMapBuilder.build();

        var expectedLabels = new HashSet<NodeLabel>();
        expectedLabels.addAll(labels1);
        expectedLabels.addAll(labels2);
        assertEquals(expectedLabels, idMap.availableNodeLabels());

        idMap.forEachNode(nodeId -> {
            var labels = idMap.toOriginalNodeId(nodeId) % 2 == 0
                ? labels1
                : labels2;

            assertEquals(labels, idMap.nodeLabels(nodeId));

            return true;
        });
    }
}
