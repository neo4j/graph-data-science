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
package org.neo4j.graphalgo.core.huge;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.NodesBuilder;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.loading.construction.TestMethodRunner;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.core.huge.AdjacencyDecompressingReader.CHUNK_SIZE;

class TransientCsrListTest {

    static Stream<TestMethodRunner> methodRunners() {
        return TestMethodRunner.adjacencyCompressions();
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void shouldPeekValues(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 3, 3, 7});
            while (adjacencyCursor.hasNextVLong()) {
                assertEquals(adjacencyCursor.peekVLong(), adjacencyCursor.nextVLong());
            }
        });
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void shouldSkipUntilLargerValue(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertEquals(2, adjacencyCursor.skipUntil(1));
            assertEquals(2, adjacencyCursor.nextVLong());
            assertEquals(3, adjacencyCursor.nextVLong());
            assertFalse(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void shouldAdvanceUntilEqualValue(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertEquals(1, adjacencyCursor.advance(1));
            assertEquals(2, adjacencyCursor.nextVLong());
            assertEquals(2, adjacencyCursor.nextVLong());
            assertEquals(3, adjacencyCursor.nextVLong());
            assertFalse(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void advanceOutOfUpperBound(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.advance(5)).isEqualTo(3);
            assertFalse(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void advanceOutOfLowerBound(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.advance(1)).isEqualTo(1);
            assertTrue(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void skipUntilOutOfUpperBound(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.skipUntil(5)).isEqualTo(3);
            assertFalse(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void skipUntilOutOfLowerBound(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.skipUntil(1)).isEqualTo(2);
            assertTrue(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void shouldPeekAcrossBlocks(TestMethodRunner runner) {
        runner.run(() -> {
            int targetCount = 2 * CHUNK_SIZE;
            long[] targets = new long[targetCount + 1];
            Arrays.setAll(targets, i -> i);
            var adjacencyCursor = adjacencyCursorFromTargets(targets);
            int position = 0;
            while (adjacencyCursor.hasNextVLong() && position < targetCount) {
                assertThat(adjacencyCursor.peekVLong()).isEqualTo(position);
                assertThat(adjacencyCursor.nextVLong()).isEqualTo(position);
                position++;
            }

            assertEquals(1, adjacencyCursor.remaining());
            assertEquals(targetCount, adjacencyCursor.peekVLong());
            assertEquals(targetCount, adjacencyCursor.peekVLong());
            assertEquals(targetCount, adjacencyCursor.nextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void shouldNextAcrossBlocks(TestMethodRunner runner) {
        runner.run(() -> {
            int targetCount = 2 * CHUNK_SIZE;
            long[] targets = new long[targetCount + 1];
            Arrays.setAll(targets, i -> i);
            var adjacencyCursor = adjacencyCursorFromTargets(targets);
            int position = 0;
            while (adjacencyCursor.hasNextVLong() && position < CHUNK_SIZE) {
                assertThat(adjacencyCursor.peekVLong()).isEqualTo(position);
                assertThat(adjacencyCursor.nextVLong()).isEqualTo(position);
                assertThat(adjacencyCursor.peekVLong()).isEqualTo(position + 1);
                position++;
            }

            while (adjacencyCursor.hasNextVLong() && position < 2 * CHUNK_SIZE) {
                assertThat(adjacencyCursor.peekVLong()).isEqualTo(position);
                assertThat(adjacencyCursor.nextVLong()).isEqualTo(position);
                position++;
            }

            assertEquals(1, adjacencyCursor.remaining());
            assertEquals(targetCount, adjacencyCursor.nextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("methodRunners")
    void shouldWorkWithVeryDenseNodes(TestMethodRunner runner) {
        runner.run(() -> {
            int nodeCount = 1_000_000;
            var tracker = AllocationTracker.empty();

            var nodesBuilder = GraphFactory.initNodesBuilder()
                .nodeCount(nodeCount)
                .maxOriginalId(nodeCount)
                .hasLabelInformation(false)
                .tracker(tracker)
                .build();

            for (int i = 0; i < nodeCount; i++) {
                nodesBuilder.addNode(i);
            }

            var nodes = nodesBuilder.build();

            var relsBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(nodes.nodeMapping())
                .orientation(Orientation.UNDIRECTED)
                .tracker(tracker)
                .build();

            for (int i = 1; i <= 200_000; i++) {
                relsBuilder.add(0, i);
            }

            for (int i = 2; i < nodeCount; i++) {
                relsBuilder.add(1, i);
            }

            for (int i = 3; i < 200_000; i++) {
                relsBuilder.add(2, i + 100_000);
            }

            var rels = relsBuilder.build();

            var graph = GraphFactory.create(nodes.nodeMapping(), rels, tracker);

            assertThat(graph.nodeCount()).isEqualTo(nodeCount);

            assertThat(graph.degree(0)).isEqualTo(200_000);
            assertThat(graph.degree(1)).isEqualTo(nodeCount - 1);
            assertThat(graph.degree(2)).isEqualTo(200_000 - 1);


            var sum0 = new MutableLong(0);
            graph.forEachRelationship(0, (sourceNodeId, targetNodeId) -> {
                sum0.add(targetNodeId);
                return true;
            });
            assertThat(sum0.longValue()).isEqualTo(LongStream.rangeClosed(1, 200_000).sum());


            var sum1 = new MutableLong(0);
            graph.forEachRelationship(1, (sourceNodeId, targetNodeId) -> {
                sum1.add(targetNodeId);
                return true;
            });
            assertThat(sum1.longValue()).isEqualTo(LongStream.range(2, nodeCount).sum());


            var sum2 = new MutableLong(0);
            graph.forEachRelationship(2, (sourceNodeId, targetNodeId) -> {
                sum2.add(targetNodeId);
                return true;
            });
            assertThat(sum2.longValue())
                .isEqualTo(LongStream.range(3, 200_000).map(i -> i + 100_000).sum() + /* undirected from 1 */ 1);
        });
    }


    static AdjacencyCursor adjacencyCursorFromTargets(long[] targets) {
        long sourceNodeId = targets[0];
        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(targets[targets.length - 1])
            .tracker(AllocationTracker.empty())
            .build();

        for (long target : targets) {
            nodesBuilder.addNode(target);
        }
        NodeMapping idMap = nodesBuilder.build().nodeMapping();

        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .concurrency(1)
            .executorService(Pools.DEFAULT)
            .tracker(AllocationTracker.empty())
            .build();

        for (long target : targets) {
            relationshipsBuilder.add(sourceNodeId, target);
        }
        Relationships relationships = relationshipsBuilder.build();
        var mappedNodeId = idMap.toMappedNodeId(sourceNodeId);
        var adjacencyList = relationships.topology().adjacencyList();
        return adjacencyList.adjacencyCursor(mappedNodeId);
    }
}
