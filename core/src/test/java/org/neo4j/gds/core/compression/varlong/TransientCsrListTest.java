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
package org.neo4j.gds.core.compression.varlong;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.TestMethodRunner;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.api.AdjacencyCursor.NOT_FOUND;
import static org.neo4j.gds.core.compression.varlong.AdjacencyDecompressingReader.CHUNK_SIZE;

class TransientCsrListTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
    void shouldPeekValues(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 3, 3, 7});
            while (adjacencyCursor.hasNextVLong()) {
                assertEquals(adjacencyCursor.peekVLong(), adjacencyCursor.nextVLong());
            }
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
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
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
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
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
    void advanceOutOfUpperBound(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.advance(5)).isEqualTo(NOT_FOUND);
            assertFalse(adjacencyCursor.hasNextVLong());

            adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.advance(3)).isEqualTo(3);
            assertFalse(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
    void advanceOutOfLowerBound(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.advance(1)).isEqualTo(1);
            assertTrue(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
    void skipUntilOutOfUpperBound(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.skipUntil(5)).isEqualTo(NOT_FOUND);
            assertFalse(adjacencyCursor.hasNextVLong());

            adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.skipUntil(3)).isEqualTo(NOT_FOUND);
            assertFalse(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
    void skipUntilOutOfLowerBound(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 2, 3});
            assertThat(adjacencyCursor.skipUntil(1)).isEqualTo(2);
            assertTrue(adjacencyCursor.hasNextVLong());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
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
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
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
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
    void advanceBy(TestMethodRunner runner) {
        runner.run(() -> {
            var adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 3, 4});
            assertThat(adjacencyCursor.advanceBy(0)).isEqualTo(0);
            assertThat(adjacencyCursor.nextVLong()).isEqualTo(1);
            assertTrue(adjacencyCursor.hasNextVLong());

            adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 3, 4});
            assertThat(adjacencyCursor.advanceBy(1)).isEqualTo(1);
            assertThat(adjacencyCursor.nextVLong()).isEqualTo(2);
            assertTrue(adjacencyCursor.hasNextVLong());

            adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 3, 4});
            assertThat(adjacencyCursor.advanceBy(2)).isEqualTo(2);
            assertThat(adjacencyCursor.nextVLong()).isEqualTo(3);
            assertTrue(adjacencyCursor.hasNextVLong());

            adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 3, 4});
            assertThat(adjacencyCursor.advanceBy(3)).isEqualTo(3);
            assertThat(adjacencyCursor.nextVLong()).isEqualTo(4);
            assertFalse(adjacencyCursor.hasNextVLong());

            adjacencyCursor = adjacencyCursorFromTargets(new long[]{0, 1, 2, 3, 4});
            assertThat(adjacencyCursor.advanceBy(5)).isEqualTo(NOT_FOUND);
            assertFalse(adjacencyCursor.hasNextVLong());
        });
    }

    static Stream<Arguments> testRunnersAndDegrees() {
        return TestSupport.crossArguments(
            () -> TestMethodRunner.adjacencyCompressions().map(Arguments::of),
            () -> Stream.of(
                // creates 3 pages .. page0: 200_000, page1 (oversize): node count, page2: 200_000
                Arguments.of(200_000, 200_000),
                // creates 2 pages .. page0: 1337, page1 (oversize): node count, page0 (reuse): 42
                Arguments.of(1337, 42)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("testRunnersAndDegrees")
    void shouldWorkWithVeryDenseNodes(TestMethodRunner runner, long firstDegree, long secondDegree) {
        runner.run(() -> {
            int nodeCount = 1_000_000;

            var nodesBuilder = GraphFactory.initNodesBuilder()
                .nodeCount(nodeCount)
                .maxOriginalId(nodeCount)
                .hasLabelInformation(false)
                .build();

            for (int i = 0; i < nodeCount; i++) {
                nodesBuilder.addNode(i);
            }

            var nodes = nodesBuilder.build();

            var relsBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(nodes.idMap())
                .relationshipType(RelationshipType.of("REL"))
                .orientation(Orientation.UNDIRECTED)
                .build();

            for (int i = 1; i <= firstDegree; i++) {
                relsBuilder.add(0, i);
            }

            for (int i = 2; i < nodeCount; i++) {
                relsBuilder.add(1, i);
            }

            for (int i = 3; i < secondDegree; i++) {
                relsBuilder.add(2, i + 100_000);
            }

            var rels = relsBuilder.build();

            var graph = GraphFactory.create(nodes.idMap(), rels);

            assertThat(graph.nodeCount()).isEqualTo(nodeCount);

            assertThat(graph.degree(0)).isEqualTo(firstDegree);
            assertThat(graph.degree(1)).isEqualTo(nodeCount - 1);
            assertThat(graph.degree(2)).isEqualTo(secondDegree - 1);


            var sum0 = new MutableLong(0);
            graph.forEachRelationship(0, (sourceNodeId, targetNodeId) -> {
                sum0.add(targetNodeId);
                return true;
            });
            assertThat(sum0.longValue()).isEqualTo(LongStream.rangeClosed(1, firstDegree).sum());


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
                .isEqualTo(LongStream.range(3, secondDegree).map(i -> i + 100_000).sum() + /* undirected from 1 */ 1);
        });
    }

    static AdjacencyCursor adjacencyCursorFromTargets(long[] targets) {
        long sourceNodeId = targets[0];
        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(targets[targets.length - 1])
            .build();

        for (long target : targets) {
            nodesBuilder.addNode(target);
        }
        IdMap idMap = nodesBuilder.build().idMap();

        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .relationshipType(RelationshipType.of("REL"))
            .concurrency(1)
            .executorService(Pools.DEFAULT)
            .build();

        for (long target : targets) {
            relationshipsBuilder.add(sourceNodeId, target);
        }
        var relationships = relationshipsBuilder.build();
        var mappedNodeId = idMap.toMappedNodeId(sourceNodeId);
        var adjacencyList = relationships.topology().adjacencyList();
        return adjacencyList.adjacencyCursor(mappedNodeId);
    }
}
