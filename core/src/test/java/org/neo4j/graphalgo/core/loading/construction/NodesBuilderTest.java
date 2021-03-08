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
package org.neo4j.graphalgo.core.loading.construction;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.HashSet;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NodesBuilderTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.core.loading.construction.TestMethodRunner#idMapImplementation")
    void parallelIdMapBuilder(TestMethodRunner runTest) {
        runTest.run(() -> {
            long nodeCount = 100;
            int concurrency = 4;
            var nodesBuilder = GraphFactory.initNodesBuilder()
                .maxOriginalId(nodeCount)
                .concurrency(concurrency)
                .tracker(AllocationTracker.empty())
                .build();

            ParallelUtil.parallelStreamConsume(
                LongStream.range(0, nodeCount),
                concurrency,
                stream -> stream.forEach(nodesBuilder::addNode)
            );

            var idMap = nodesBuilder.build();

            assertEquals(nodeCount, idMap.nodeCount());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.core.loading.construction.TestMethodRunner#idMapImplementation")
    void parallelIdMapBuilderWithDuplicateNodes(TestMethodRunner runTest) {
        runTest.run(() -> {
            long attempts = 100;
            int concurrency = 4;
            var nodesBuilder = GraphFactory.initNodesBuilder()
                .maxOriginalId(attempts)
                .concurrency(concurrency)
                .tracker(AllocationTracker.empty())
                .build();

            ParallelUtil.parallelStreamConsume(
                LongStream.range(0, attempts),
                concurrency,
                stream -> stream.forEach(originalId -> nodesBuilder.addNode(0))
            );

            var idMap = nodesBuilder.build();

            assertEquals(1, idMap.nodeCount());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.core.loading.construction.TestMethodRunner#idMapImplementation")
    void parallelIdMapBuilderWithLabels(TestMethodRunner runTest) {
        runTest.run(() -> {
            long attempts = 100;
            int concurrency = 1;
            var labels1 = new HashSet<>(NodeLabel.listOf("Label1"));
            var labels2 = new HashSet<>(NodeLabel.listOf("Label2"));

            var nodesBuilder = GraphFactory.initNodesBuilder()
                .maxOriginalId(attempts)
                .hasLabelInformation(true)
                .concurrency(concurrency)
                .tracker(AllocationTracker.empty())
                .build();

            ParallelUtil.parallelStreamConsume(LongStream.range(0, attempts), concurrency, stream -> stream.forEach(
                originalId -> {
                    var labels = originalId % 2 == 0
                        ? labels1.toArray(NodeLabel[]::new)
                        : labels2.toArray(NodeLabel[]::new);

                    nodesBuilder.addNode(originalId, labels);
                })
            );

            var idMap = nodesBuilder.build();

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
        });
    }
}
