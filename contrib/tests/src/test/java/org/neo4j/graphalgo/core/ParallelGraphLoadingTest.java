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
package org.neo4j.graphalgo.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.PrivateLookup;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphalgo.core.utils.RawValues.combineIntInt;

@RunWith(Parameterized.class)
public class ParallelGraphLoadingTest extends RandomGraphTestCase {

    @Rule
    public Timeout timeout = new Timeout(5, TimeUnit.SECONDS);

    private final int batchSize;
    private final Class<? extends GraphFactory> graphImpl;

    @Parameters(name = "{2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{30, HeavyGraphFactory.class, "Heavy, parallel"},
                new Object[]{100000, HeavyGraphFactory.class, "Heavy, sequential"},
                new Object[]{30, HugeGraphFactory.class, "Huge, parallel"},
                new Object[]{100000, HugeGraphFactory.class, "Huge, sequential"}
        );
    }

    private Graph graph;

    public ParallelGraphLoadingTest(
            int batchSize,
            Class<? extends GraphFactory> graphImpl,
            String ignoredNameForNiceTestDisplay) {
        this.batchSize = batchSize;
        this.graphImpl = graphImpl;
        graph = load();
    }

    @Test
    public void shouldLoadAllNodes() throws Exception {
        assertEquals(NODE_COUNT, graph.nodeCount());
    }

    @Test
    public void shouldLoadSparseNodes() throws Exception {
        GraphDatabaseAPI largerGraph = buildGraph(PageUtil.pageSizeFor(Long.BYTES) << 1);
        try {
            Graph sparseGraph = load(largerGraph, l -> l.withLabel("Label2"));
            try (Transaction tx = largerGraph.beginTx();
                 Stream<Node> nodes = largerGraph
                         .findNodes(Label.label("Label2"))
                         .stream()) {
                nodes.forEach(n -> {
                    long graphId = sparseGraph.toMappedNodeId(n.getId());
                    assertNotEquals(n + " not mapped", -1, graphId);
                    long neoId = sparseGraph.toOriginalNodeId(graphId);
                    assertEquals(n + " mapped wrongly", n.getId(), neoId);
                });
                tx.success();
            }
        } finally {
            largerGraph.shutdown();
        }
    }

    @Test
    public void shouldLoadNodesInOrder() throws Exception {
        if (batchSize < NODE_COUNT) {
            graph.forEachNode(nodeId -> {
                assertEquals(
                        nodeId,
                        graph.toOriginalNodeId(nodeId));
                return true;
            });
        } else {
            final Set<Long> nodeIds;
            try (Transaction tx = db.beginTx()) {
                nodeIds = db.getAllNodes().stream()
                        .map(Node::getId)
                        .collect(Collectors.toSet());
                tx.success();
            }

            graph.forEachNode(nodeId -> {
                assertEquals(
                        true,
                        nodeIds.remove(graph.toOriginalNodeId(nodeId)));
                assertEquals(
                        nodeId,
                        graph.toMappedNodeId(graph.toOriginalNodeId(nodeId)));
                return true;
            });
        }
    }

    @Test
    public void shouldLoadAllRelationships() throws Exception {
        try (Transaction tx = db.beginTx()) {
            graph.forEachNode(this::testRelationships);
            tx.success();
        }
    }

    @Test
    public void shouldCollectErrors() throws Exception {
        if (batchSize < NODE_COUNT) {
            String message = "oh noes";
            ThrowingThreadPool pool = new ThrowingThreadPool(3, message);
            try {
                new GraphLoader(db, pool)
                        .withBatchSize(batchSize)
                        .load(graphImpl);
                fail("Should have thrown an Exception.");
            } catch (Exception e) {
                assertEquals(message, e.getMessage());
                assertEquals(RuntimeException.class, e.getClass());
                final Throwable[] suppressed = e.getSuppressed();
                for (Throwable t : suppressed) {
                    assertEquals(message, t.getMessage());
                    assertEquals(RuntimeException.class, t.getClass());
                }
            }
        }
    }

    private boolean testRelationships(long nodeId) {
        testRelationships(nodeId, Direction.OUTGOING);
        testRelationships(nodeId, Direction.INCOMING);
        return true;
    }

    private void testRelationships(long nodeId, final Direction direction) {
        final Node node = db.getNodeById(graph.toOriginalNodeId(nodeId));
        final Map<Long, Relationship> relationships = Iterables
                .stream(node.getRelationships(direction))
                .collect(Collectors.toMap(
                        rel -> combineIntInt((int) rel.getStartNode().getId(), (int) rel.getEndNode().getId()),
                        Function.identity()));
        graph.forEachRelationship(
                nodeId,
                direction,
                (sourceId, targetId) -> {
                    assertEquals(nodeId, sourceId);
                    long relId = 0;
                    switch (direction) {
                        case OUTGOING:
                            relId = combineIntInt((int) sourceId, (int) targetId);
                            break;
                        case INCOMING:
                            relId = combineIntInt((int) targetId, (int) sourceId);
                            break;
                    }
                    final Relationship relationship = relationships.remove(relId);
                    assertNotNull(
                            String.format(
                                    "Relationship (%d)-[%d]->(%d) that does not exist in the graph",
                                    sourceId,
                                    relId,
                                    targetId),
                            relationship);

                    if (direction == Direction.OUTGOING) {
                        assertEquals(
                                relationship.getStartNode().getId(),
                                graph.toOriginalNodeId(sourceId));
                        assertEquals(
                                relationship.getEndNode().getId(),
                                graph.toOriginalNodeId(targetId));
                    } else {
                        assertEquals(
                                relationship.getEndNode().getId(),
                                graph.toOriginalNodeId(sourceId));
                        assertEquals(
                                relationship.getStartNode().getId(),
                                graph.toOriginalNodeId(targetId));
                    }
                    return true;
                });

        assertTrue(
                "Relationships that were not traversed " + relationships,
                relationships.isEmpty());
    }

    private Graph load() {
        return load(db, l -> {});
    }

    private Graph load(GraphDatabaseAPI db, Consumer<GraphLoader> block) {
        final ExecutorService pool = Executors.newFixedThreadPool(3);
        GraphLoader loader = new GraphLoader(db, pool).withBatchSize(batchSize);
        block.accept(loader);
        try {
            return loader.load(graphImpl);
        } catch (Exception e) {
            markFailure();
            throw e;
        } finally {
            pool.shutdown();
        }
    }

    private static class ThrowingThreadPool extends ThreadPoolExecutor {
        private static final MethodHandle setException = PrivateLookup.method(
                FutureTask.class,
                "setException",
                MethodType.methodType(void.class, Throwable.class));
        private final String message;

        private ThrowingThreadPool(int numberOfThreads, String message) {
            super(
                    numberOfThreads,
                    numberOfThreads,
                    1,
                    TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>());
            this.message = message;
        }

        @Override
        public Future<?> submit(final Runnable task) {
            final CompletableFuture<Object> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException(message));
            return future;
        }

        @Override
        public void execute(final Runnable command) {
            if (command instanceof FutureTask) {
                FutureTask<?> future = (FutureTask<?>) command;
                try {
                    setException.invoke(future, new RuntimeException(message));
                } catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }
            }
        }
    }
}
