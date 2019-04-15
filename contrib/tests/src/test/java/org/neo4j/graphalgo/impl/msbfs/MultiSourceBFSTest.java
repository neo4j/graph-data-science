/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.neo4jview.DirectIdMapping;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class MultiSourceBFSTest {

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

    @Test
    public void testPaperExample() {
        withGraph(PAPER_CYPHER, graph -> {
            BfsConsumer mock = mock(BfsConsumer.class);
            MultiSourceBFS msbfs = new MultiSourceBFS(
                    graph,
                    graph,
                    OUTGOING,
                    (i, d, s) -> mock.accept(i + 1, d, toList(s, x -> x + 1)),
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
                    (i, d, s) -> mock.accept(i + 1, d, toList(s, x -> x + 1))
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
                    Set<Pair<Integer, Integer>> seen = new HashSet<>();
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
                            }
                    );
                    msbfs.run(1, null);
                });
    }

    @Test
    public void testParallel() {
        // each node should only be traversed once for every source node
        int maxNodes = 512;
        int[][] seen = new int[maxNodes][];
        Arrays.setAll(seen, i -> new int[maxNodes]);
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
                                        seen[s.next()][i] += 1;
                                    }
                                }
                            });
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
        // [ last i, expected source from, exptected source to ]
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
                                state[0] = i;
                                int sourceFrom = state[1];
                                int sourceTo = state[2];

                                int expectedSize = sourceTo - sourceFrom;
                                if (i >= sourceFrom && i < sourceTo) {
                                    // if the current node is in the sources
                                    // if will not be traversed
                                    expectedSize -= 1;
                                }

                                assertEquals(expectedSize, s.size());
                            });
                    // run sequentially to guarantee order
                    msbfs.run(1, null);
                });
    }

    @Test
    public void testLarger() {
        final int nodeCount = 8192;
        final int sourceCount = 1024;

        RelationshipIterator iter = (nodeId, direction, consumer) -> {
            for (int i = 0; i < nodeCount; i++) {
                if (i != nodeId) {
                    consumer.accept(nodeId, i, -1L);
                }
            }
        };

        final int[] sources = new int[sourceCount];
        Arrays.setAll(sources, i -> i);
        final int[][] seen = new int[nodeCount][sourceCount];
        MultiSourceBFS msbfs = new MultiSourceBFS(
                new DirectIdMapping(nodeCount),
                iter,
                Direction.OUTGOING,
                (nodeId, depth, sourceNodeIds) -> {
                    assertEquals(1, depth);
                    synchronized (seen) {
                        final int[] nodeSeen = seen[nodeId];
                        while (sourceNodeIds.hasNext()) {
                            nodeSeen[sourceNodeIds.next()] += 1;
                        }
                    }
                },
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
        block.accept(new GraphLoader(db).load(HeavyGraphFactory.class));
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
        Graph graph = new GraphLoader(db).load(HeavyGraphFactory.class);
        block.accept(graph);
    }

    private static BfsSources toList(
            BfsSources sources,
            IntUnaryOperator modify) {
        List<Integer> ints = new ArrayList<>();
        while (sources.hasNext()) {
            ints.add(modify.applyAsInt(sources.next()));
        }
        return new FakeListIterator(ints);
    }

    private static BfsSources toList(int... sources) {
        List<Integer> ints = new ArrayList<>();
        for (int source : sources) {
            ints.add(source);
        }
        return new FakeListIterator(ints);
    }

    private static final class FakeListIterator implements BfsSources {

        private List<?> ints;

        private FakeListIterator(List<Integer> ints) {
            ints.sort(Integer::compareTo);
            this.ints = ints;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public int next() {
            return 0;
        }

        @Override
        public int size() {
            return ints.size();
        }

        @Override
        public void reset() {}

        @Override
        public boolean equals(final Object obj) {
            return obj instanceof FakeListIterator && ints.equals(((FakeListIterator) obj).ints);
        }

        @Override
        public String toString() {
            return ints.toString();
        }
    }
}
