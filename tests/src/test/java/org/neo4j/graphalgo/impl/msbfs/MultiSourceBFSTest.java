/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.msbfs;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphalgo.HeavyHugeTester;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.DirectIdMapping;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class MultiSourceBFSTest extends HeavyHugeTester {

    private static final String PAPER_CYPHER = "" +
            "CREATE (a:Foo {id:\"1\"})\n" +
            "CREATE (b:Foo {id:\"2\"})\n" +
            "CREATE (c:Foo {id:\"3\"})\n" +
            "CREATE (d:Foo {id:\"4\"})\n" +
            "CREATE (e:Foo {id:\"5\"})\n" +
            "CREATE (f:Foo {id:\"6\"})\n" +
            "CREATE\n" +
            "  (a)-[:BAR]->(c),\n" +
            "  (a)-[:BAR]->(d),\n" +
            "  (b)-[:BAR]->(c),\n" +
            "  (b)-[:BAR]->(d),\n" +
            "  (c)-[:BAR]->(a),\n" +
            "  (c)-[:BAR]->(b),\n" +
            "  (c)-[:BAR]->(e),\n" +
            "  (d)-[:BAR]->(a),\n" +
            "  (d)-[:BAR]->(b),\n" +
            "  (d)-[:BAR]->(f),\n" +
            "  (e)-[:BAR]->(c),\n" +
            "  (f)-[:BAR]->(d)\n";

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    public MultiSourceBFSTest(Class<? extends GraphFactory> graphImpl, String name) {
        super(graphImpl);
    }

    @Test
    public void testPaperExample() {
        withGraph(PAPER_CYPHER, graph -> {
            BfsConsumer mock = mock(BfsConsumer.class);
            MultiSourceBFS msbfs = new MultiSourceBFS(
                    graph,
                    graph,
                    OUTGOING,
                    (i, d, s) -> mock.accept(i + 1, d, toList(s, x -> x + 1)),
                    AllocationTracker.EMPTY,
                    0, 1
            );

            msbfs.run(Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT);

            verify(mock).accept(3, 1, toList(1, 2));
            verify(mock).accept(4, 1, toList(1, 2));
            verify(mock).accept(5, 2, toList(1, 2));
            verify(mock).accept(6, 2, toList(1, 2));
            verify(mock).accept(1, 2, toList(2));
            verify(mock).accept(2, 2, toList(1));
            verifyNoMoreInteractions(mock);
        });
    }

    @Test
    public void testPaperExampleWithAllSources() {
        withGraph(PAPER_CYPHER, graph -> {
            BfsConsumer mock = mock(BfsConsumer.class);
            MultiSourceBFS msbfs = new MultiSourceBFS(
                    graph,
                    graph,
                    OUTGOING,
                    (i, d, s) -> mock.accept(i + 1, d, toList(s, x -> x + 1)),
                    AllocationTracker.EMPTY
            );

            msbfs.run(Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT);

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
        });
    }

    @Test
    public void testSequentialInvariant() {
        // for a single run with < ω nodes, the same node may only be traversed once at a given depth
        withGrid(
                gb -> gb.newGridBuilder().createGrid(8, 4),
                graph -> {
                    Set<Pair<Long, Integer>> seen = new HashSet<>();
                    MultiSourceBFS msbfs = new MultiSourceBFS(
                            graph,
                            graph,
                            OUTGOING,
                            (i, d, s) -> {
                                String message = String.format(
                                        "The node(%d) was traversed multiple times at depth %d",
                                        i,
                                        d
                                );
                                assertTrue(message, seen.add(Pair.of(i, d)));
                            },
                            AllocationTracker.EMPTY
                    );
                    msbfs.run(1, null);
                });
    }

    @Test
    public void testParallel() {
        // each node should only be traversed once for every source node
        int maxNodes = 512;
        int[][] seen = new int[maxNodes][maxNodes];
        withGrid(
                gb -> gb.newCompleteGraphBuilder().createCompleteGraph(maxNodes),
                graph -> {
                    MultiSourceBFS msbfs = new MultiSourceBFS(
                            graph,
                            graph,
                            OUTGOING,
                            (i, d, s) -> {
                                assertEquals(1, d);
                                synchronized (seen) {
                                    while (s.hasNext()) {
                                        seen[(int) s.next()][(int) i] += 1;
                                    }
                                }
                            },
                            AllocationTracker.EMPTY);
                    msbfs.run(Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT);
                });

        for (int i = 0; i < maxNodes; i++) {
            final int[] ints = seen[i];
            int[] expected = new int[maxNodes];
            Arrays.fill(expected, 1);
            expected[i] = 0; // MS-BFS does not call fn for start nodes
            assertArrayEquals(expected, ints);
        }
    }

    @Test
    public void testSize() {
        int maxNodes = 100;
        // [ last i, expected source from, expected source to ]
        int[] state = {-1, 0, MultiSourceBFS.OMEGA};
        withGrid(
                gb -> gb.newCompleteGraphBuilder().createCompleteGraph(maxNodes),
                graph -> {
                    MultiSourceBFS msbfs = new MultiSourceBFS(
                            graph,
                            graph,
                            OUTGOING,
                            (i, d, s) -> {
                                int prev = state[0];
                                if (i < prev) {
                                    // we complete a source chunk and start again for the next one
                                    state[1] = state[2];
                                    state[2] = Math.min(
                                            state[2] + MultiSourceBFS.OMEGA,
                                            maxNodes);
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
                            AllocationTracker.EMPTY);
                    // run sequentially to guarantee order
                    msbfs.run(1, null);
                });
    }

    @Test
    public void testLarger() throws Exception {
        final int nodeCount = 8192;
        final int sourceCount = 1024;

        RelationshipIterator iter = new RelationshipIterator() {
            @Override
            public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
                for (long i = 0; i < nodeCount; i++) {
                    if (i != nodeId) {
                        consumer.accept(nodeId, i);
                    }
                }
            }

            @Override
            public void forEachRelationship(long nodeId, Direction direction, WeightedRelationshipConsumer consumer) {

            }

            @Override
            public boolean exists(final long sourceNodeId, final long targetNodeId, final Direction direction) {
                return false;
            }
        } ;

        final long[] sources = new long[sourceCount];
        Arrays.setAll(sources, i -> i);
        final int[][] seen = new int[nodeCount][sourceCount];
        MultiSourceBFS msbfs = new MultiSourceBFS(
                new DirectIdMapping(nodeCount),
                iter,
                Direction.OUTGOING,
                (nodeId, depth, sourceNodeIds) -> {
                    assertEquals(1, depth);
                    synchronized (seen) {
                        final int[] nodeSeen = seen[(int) nodeId];
                        while (sourceNodeIds.hasNext()) {
                            nodeSeen[(int) sourceNodeIds.next()] += 1;
                        }
                    }
                },
                AllocationTracker.EMPTY,
                sources);
        msbfs.run(Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT);

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

    private void withGraph(
            String cypher,
            Consumer<? super Graph> block) {
                db.execute(cypher).close();
        block.accept(new GraphLoader(db).load(graphImpl));
    }

    private void withGrid(
            Consumer<? super GraphBuilder<?>> build,
            Consumer<? super Graph> block) {
        db.executeAndCommit((dba) -> {
                DefaultBuilder graphBuilder = GraphBuilder.create(db)
                        .setLabel("Foo")
                        .setRelationship("BAR");
                build.accept(graphBuilder);
        });
        Graph graph = new GraphLoader(db).load(graphImpl);
                block.accept(graph);
    }

    private static BfsSources toList(
            BfsSources sources,
            LongUnaryOperator modify) {
        List<Long> longs = new ArrayList<>();
        while (sources.hasNext()) {
            longs.add(modify.applyAsLong(sources.next()));
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

        private List<?> longs;

        private FakeListIterator(List<Long> longs) {
            longs.sort(Long::compareTo);
            this.longs = longs;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public long next() {
            return 0;
        }

        @Override
        public int size() {
            return longs.size();
        }

        @Override
        public void reset() {}

        @Override
        public boolean equals(final Object obj) {
            return obj instanceof FakeListIterator && longs.equals(((FakeListIterator) obj).longs);
        }

        @Override
        public String toString() {
            return longs.toString();
        }
    }
}
