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
package org.neo4j.gds.msbfs;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongUnaryOperator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class MultiSourceBFSAccessMethodsTest {

    @GdlExtension
    @Nested
    class sampleTest {

        @GdlGraph(orientation = Orientation.UNDIRECTED)
        public static final String DB_CYPHER =
            "CREATE" +
                "  (a:Foo {id: 1})" +
                ", (b:Foo {id: 2})" +
                ", (c:Foo {id: 3})" +
                ", (d:Foo {id: 4})" +
                ", (e:Foo {id: 5})" +
                ", (f:Foo {id: 6})" +

                ", (a)-[:BAR]->(c)" +
                ", (a)-[:BAR]->(d)" +
                ", (b)-[:BAR]->(c)" +
                ", (b)-[:BAR]->(d)" +
                ", (c)-[:BAR]->(e)" +
                ", (d)-[:BAR]->(f)";

        @Inject
        private Graph graph;

        @Test
        void testWithPredecessor() {

            BfsConsumer bfsConsumerMock = mock(BfsConsumer.class);
            BfsWithPredecessorConsumer bfsWithPredecessorConsumerMock = mock(BfsWithPredecessorConsumer.class);
            MultiSourceBFSAccessMethods msbfs = MultiSourceBFSAccessMethods.predecessorProcessing(
                graph,
                (i, d, s) -> bfsConsumerMock.accept(i + 1, d, toList(s, x -> x + 1)),
                (i, p, d, s) -> bfsWithPredecessorConsumerMock.accept(i + 1, p + 1, d, toList(s, x -> x + 1)),
                Optional.of(new long[]{0, 1}),
                TerminationFlag.RUNNING_TRUE
            );

            msbfs.run(ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY, DefaultPool.INSTANCE);

            verify(bfsConsumerMock).accept(1, 0, toList(1));
            verify(bfsConsumerMock).accept(2, 0, toList(2));
            verify(bfsConsumerMock).accept(3, 1, toList(1, 2));
            verify(bfsConsumerMock).accept(4, 1, toList(1, 2));
            verify(bfsConsumerMock).accept(1, 2, toList(2));
            verify(bfsConsumerMock).accept(2, 2, toList(1));
            verify(bfsConsumerMock).accept(5, 2, toList(1, 2));
            verify(bfsConsumerMock).accept(6, 2, toList(1, 2));

            verify(bfsWithPredecessorConsumerMock).accept(3, 1, 1, toList(1));
            verify(bfsWithPredecessorConsumerMock).accept(3, 2, 1, toList(2));
            verify(bfsWithPredecessorConsumerMock).accept(4, 1, 1, toList(1));
            verify(bfsWithPredecessorConsumerMock).accept(4, 2, 1, toList(2));

            verify(bfsWithPredecessorConsumerMock).accept(5, 3, 2, toList(1, 2));
            verify(bfsWithPredecessorConsumerMock).accept(6, 4, 2, toList(1, 2));

            verify(bfsWithPredecessorConsumerMock).accept(2, 3, 2, toList(1));
            verify(bfsWithPredecessorConsumerMock).accept(2, 4, 2, toList(1));

            verify(bfsWithPredecessorConsumerMock).accept(1, 3, 2, toList(2));
            verify(bfsWithPredecessorConsumerMock).accept(1, 4, 2, toList(2));

            verifyNoMoreInteractions(bfsWithPredecessorConsumerMock);

        }

        @Test
        void testWithANP() {
            BfsConsumer mock = mock(BfsConsumer.class);
            MultiSourceBFSAccessMethods msbfs = MultiSourceBFSAccessMethods.aggregatedNeighborProcessing(
                graph.nodeCount(),
                graph,
                (i, d, s) -> mock.accept(i + 1, d, toList(s, x -> x + 1)),
                Optional.of(new long[]{0, 1}),
                TerminationFlag.RUNNING_TRUE
            );

            msbfs.run(ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY, DefaultPool.INSTANCE);

            verify(mock).accept(3, 1, toList(1, 2));
            verify(mock).accept(4, 1, toList(1, 2));
            verify(mock).accept(5, 2, toList(1, 2));
            verify(mock).accept(6, 2, toList(1, 2));
            verify(mock).accept(1, 2, toList(2));
            verify(mock).accept(2, 2, toList(1));
            verifyNoMoreInteractions(mock);

        }

        @Test
        void testPredecessorWithAllSources() {
            BfsWithPredecessorConsumer mock = mock(BfsWithPredecessorConsumer.class);
            MultiSourceBFSAccessMethods msbfs = MultiSourceBFSAccessMethods.predecessorProcessing(
                graph,
                (i, d, s) -> {},
                (i, p, d, s) -> mock.accept(i + 1, p + 1, d, toList(s, x -> x + 1)),
                Optional.empty(),
                TerminationFlag.RUNNING_TRUE
            );

            msbfs.run(ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY, DefaultPool.INSTANCE);

            verify(mock).accept(1, 3, 1, toList(3));
            verify(mock).accept(1, 4, 1, toList(4));
            verify(mock).accept(2, 3, 1, toList(3));
            verify(mock).accept(2, 4, 1, toList(4));
            verify(mock).accept(3, 1, 1, toList(1));
            verify(mock).accept(3, 2, 1, toList(2));
            verify(mock).accept(3, 5, 1, toList(5));
            verify(mock).accept(4, 1, 1, toList(1));
            verify(mock).accept(4, 2, 1, toList(2));
            verify(mock).accept(4, 6, 1, toList(6));
            verify(mock).accept(5, 3, 1, toList(3));
            verify(mock).accept(6, 4, 1, toList(4));

            verify(mock).accept(1, 3, 2, toList(2, 5));
            verify(mock).accept(1, 4, 2, toList(2, 6));
            verify(mock).accept(2, 3, 2, toList(1, 5));
            verify(mock).accept(2, 4, 2, toList(1, 6));
            verify(mock).accept(3, 1, 2, toList(4));
            verify(mock).accept(3, 2, 2, toList(4));
            verify(mock).accept(4, 1, 2, toList(3));
            verify(mock).accept(4, 2, 2, toList(3));
            verify(mock).accept(5, 3, 2, toList(1, 2));
            verify(mock).accept(6, 4, 2, toList(1, 2));

            verify(mock).accept(3, 1, 3, toList(6));
            verify(mock).accept(3, 2, 3, toList(6));
            verify(mock).accept(4, 1, 3, toList(5));
            verify(mock).accept(4, 2, 3, toList(5));
            verify(mock).accept(5, 3, 3, toList(4));
            verify(mock).accept(6, 4, 3, toList(3));

            verify(mock).accept(5, 3, 4, toList(6));
            verify(mock).accept(6, 4, 4, toList(5));

            verifyNoMoreInteractions(mock);

        }


        @Test
        void testANPWithAllSources() {

            BfsConsumer mock = mock(BfsConsumer.class);
            MultiSourceBFSAccessMethods msbfs = MultiSourceBFSAccessMethods.aggregatedNeighborProcessing(
                graph.nodeCount(),
                graph,
                (i, d, s) -> mock.accept(i + 1, d, toList(s, x -> x + 1)),
                Optional.empty(),
                TerminationFlag.RUNNING_TRUE
            );

            msbfs.run(ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY, DefaultPool.INSTANCE);

            verify(mock).accept(1, 1, toList(3, 4));
            verify(mock).accept(2, 1, toList(3, 4));
            verify(mock).accept(3, 1, toList(1, 2, 5));
            verify(mock).accept(4, 1, toList(1, 2, 6));
            verify(mock).accept(5, 1, toList(3));
            verify(mock).accept(6, 1, toList(4));

            verify(mock).accept(1, 2, toList(2, 5, 6));
            verify(mock).accept(2, 2, toList(1, 5, 6));
            verify(mock).accept(3, 2, toList(4));
            verify(mock).accept(4, 2, toList(3));
            verify(mock).accept(5, 2, toList(1, 2));
            verify(mock).accept(6, 2, toList(1, 2));

            verify(mock).accept(3, 3, toList(6));
            verify(mock).accept(4, 3, toList(5));
            verify(mock).accept(5, 3, toList(4));
            verify(mock).accept(6, 3, toList(3));

            verify(mock).accept(5, 4, toList(6));
            verify(mock).accept(6, 4, toList(5));

            verifyNoMoreInteractions(mock);

        }
    }


    @Nested
    class GridTest {

        Graph grid(long n, long m) {

            var graph = mock(Graph.class);

            doAnswer(invocation -> {
                var nodeId = (Long) invocation.getArgument(0);
                var relationshipConsumer = (RelationshipConsumer) invocation.getArgument(1);
                long i = nodeId / m;
                long j = nodeId % m;
                if (j - 1 >= 0) {
                    relationshipConsumer.accept(nodeId, nodeId - 1);
                }
                if (j + 1 < m) {
                    relationshipConsumer.accept(nodeId, nodeId + 1);
                }
                if (i - 1 >= 0) {
                    relationshipConsumer.accept(nodeId, nodeId - m);
                }
                if (i + 1 < n) {
                    relationshipConsumer.accept(nodeId, nodeId + m);
                }
                return null;
            }).when(graph).forEachRelationship(
                anyLong(),
                any(RelationshipConsumer.class)
            );

            doAnswer(invocation -> graph).when(graph).concurrentCopy();

            when(graph.nodeCount()).thenReturn(n * m);

            return graph;
        }


        @Test
        void testSequentialInvariant() {
            // for a single run with < ω nodes, the same node may only be traversed once at a given depth
            //gb -> gb.newGridBuilder().createGrid(8, 4);
            var graph = grid(8, 4);
            Set<Pair<Long, Integer>> seen = new HashSet<>();
            MultiSourceBFSAccessMethods msbfs = MultiSourceBFSAccessMethods.aggregatedNeighborProcessing(
                graph.nodeCount(),
                graph,
                (i, d, s) -> {
                    String message = formatWithLocale(
                        "The node(%d) was traversed multiple times at depth %d",
                        i,
                        d
                    );
                    assertTrue(seen.add(Pair.of(i, d)), message);
                },
                Optional.empty(),
                TerminationFlag.RUNNING_TRUE
            );
            msbfs.run(new Concurrency(1), null);

        }
    }

    @Nested
    class CompleteGraph {

        Graph completeGraph(long nodeCount) {

            var graph = mock(Graph.class);

            doAnswer(invocation -> {
                var nodeId = (Long) invocation.getArgument(0);
                var relationshipConsumer = (RelationshipConsumer) invocation.getArgument(1);
                for (long j = 0; j < nodeCount; ++j) {
                    if (nodeId != j) {
                        relationshipConsumer.accept(nodeId, j);
                    }
                }
                return null;
            }).when(graph).forEachRelationship(
                anyLong(),
                any(RelationshipConsumer.class)
            );

            doAnswer(invocation -> graph).when(graph).concurrentCopy();

            when(graph.nodeCount()).thenReturn(nodeCount);

            return graph;
        }

        @Test
        void testParallel() {
            // each node should only be traversed once for every source node
            int maxNodes = 256;
            int[][] seen = new int[maxNodes][maxNodes];
            var graph = completeGraph(maxNodes);

            var msbfs = MultiSourceBFSAccessMethods.aggregatedNeighborProcessing(
                graph.nodeCount(),
                graph,
                (i, d, s) -> {
                    assertEquals(1, d);
                    synchronized (seen) {
                        while (s.hasNext()) {
                            seen[(int) s.nextLong()][(int) i] += 1;
                        }
                    }
                },
                Optional.empty(),
                TerminationFlag.RUNNING_TRUE
            );
            msbfs.run(ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY, DefaultPool.INSTANCE);


            for (int i = 0; i < maxNodes; i++) {
                final int[] ints = seen[i];
                int[] expected = new int[maxNodes];
                Arrays.fill(expected, 1);
                expected[i] = 0; // MS-BFS does not call fn for start nodes
                assertArrayEquals(expected, ints);
            }
        }

        @Test
        void testSize() {
            int maxNodes = 100;
            // [ last i, expected source from, expected source to ]
            int[] state = {-1, 0, MSBFSConstants.OMEGA};

            var graph = completeGraph(maxNodes);

            var msbfs = MultiSourceBFSAccessMethods.aggregatedNeighborProcessing(
                graph.nodeCount(),
                graph,
                (i, d, s) -> {
                    int prev = state[0];
                    if (i < prev) {
                        // we complete a source chunk and start again for the next one
                        state[1] = state[2];
                        state[2] = Math.min(
                            state[2] + MSBFSConstants.OMEGA,
                            maxNodes
                        );
                    }
                    state[0] = (int) i;
                    int sourceFrom = state[1];
                    int sourceTo = state[2];

                    int expectedSize = sourceTo - sourceFrom;
                    if (i >= sourceFrom && i < sourceTo) {
                        // if the current node is in the sources
                        // if will not be traversed
                        expectedSize -= 1;
                    }

                    assertEquals(expectedSize, s.size());
                },
                Optional.empty(),
                TerminationFlag.RUNNING_TRUE
            );
            // run sequentially to guarantee order
            msbfs.run(new Concurrency(1), null);

        }


        @Test
        void testLarger() {
            final int nodeCount = 8192;
            final int sourceCount = 1024;

            final long[] sources = new long[sourceCount];
            Arrays.setAll(sources, i -> i);
            final int[][] seen = new int[nodeCount][sourceCount];

            BfsConsumer bfsConsumer = (nodeId, depth, sourceNodeIds) -> {
                assertEquals(1, depth);
                synchronized (seen) {
                    final int[] nodeSeen = seen[(int) nodeId];
                    while (sourceNodeIds.hasNext()) {
                        nodeSeen[(int) sourceNodeIds.nextLong()] += 1;
                    }
                }
            };

            var iter = completeGraph(nodeCount);
            MultiSourceBFSAccessMethods msbfs = MultiSourceBFSAccessMethods.aggregatedNeighborProcessing(
                nodeCount,
                iter,
                bfsConsumer,
                Optional.of(sources),
                TerminationFlag.RUNNING_TRUE
            );
            msbfs.run(ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY, DefaultPool.INSTANCE);

            for (int i = 0; i < seen.length; i++) {
                final int[] nodeSeen = seen[i];
                final int[] expected = new int[sourceCount];
                Arrays.fill(expected, 1);
                if (i < sourceCount) {
                    expected[i] = 0;
                }
                assertArrayEquals(expected, nodeSeen);
            }
        }
    }


    private static BfsSources toList(BfsSources sources, LongUnaryOperator modify) {
        List<Long> longs = new ArrayList<>();
        while (sources.hasNext()) {
            longs.add(modify.applyAsLong(sources.nextLong()));
        }
        return new FakeListIterator(longs);
    }

    private static BfsSources toList(long... sources) {
        List<Long> longs = new ArrayList<>();
        for (long source : sources) {
            longs.add(source);
        }
        return new FakeListIterator(longs);
    }

    private static final class FakeListIterator implements BfsSources {

        private final List<?> longs;

        private FakeListIterator(List<Long> longs) {
            longs.sort(Long::compareTo);
            this.longs = longs;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public long nextLong() {
            return 0;
        }

        @Override
        public int size() {
            return longs.size();
        }

        @Override
        public boolean equals(final Object obj) {
            return obj instanceof FakeListIterator && longs.equals(((FakeListIterator) obj).longs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(longs);
        }

        @Override
        public String toString() {
            return longs.toString();
        }
    }

}
