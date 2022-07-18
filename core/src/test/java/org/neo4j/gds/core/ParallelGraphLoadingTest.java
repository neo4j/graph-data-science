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
package org.neo4j.gds.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.gds.PrivateLookup;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
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
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.applyInTransaction;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.getNodeById;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.gds.core.utils.RawValues.combineIntInt;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ParallelGraphLoadingTest extends RandomGraphTestCase {

    @Test
    @Timeout(value = 5)
    void shouldLoadAllNodes() {
        Graph graph = loadEverything();
        assertEquals(NODE_COUNT, graph.nodeCount());
    }

    @Test
    @Timeout(value = 5)
    void shouldLoadNodesInOrder() {
        Graph graph = loadEverything();
        final Set<Long> nodeIds;
        nodeIds = applyInTransaction(db, tx -> tx.getAllNodes().stream()
            .map(Node::getId)
            .collect(Collectors.toSet())
        );

        graph.forEachNode(nodeId -> {
            assertTrue(nodeIds.remove(graph.toOriginalNodeId(nodeId)));
            assertEquals(
                nodeId,
                graph.toMappedNodeId(graph.toOriginalNodeId(nodeId))
            );
            return true;
        });
    }

    @Test
    @Timeout(value = 5)
    void shouldLoadAllRelationships() {
        Graph graph = loadEverything();
        runInTransaction(db, tx -> graph.forEachNode(id -> testRelationships(tx, graph, id)));
    }

    @Test
    @Timeout(value = 5)
    void shouldCollectErrors() {
        String message = "oh noes";
        ThrowingThreadPool pool = new ThrowingThreadPool(3, message);
        try {
            new StoreLoaderBuilder()
                .databaseService(db)
                .executorService(pool)
                .build()
                .graph();
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

    @Nested
    class LoadSparseNodes {

        private GraphStore sparseGraph;

        @BeforeEach
        void setUp() {
            clearDb();
            buildGraph(PageUtil.pageSizeFor(PageUtil.PAGE_SIZE_32KB, Long.BYTES) << 1);
            sparseGraph = load(db, l -> l.addNodeLabel("Label2"));

        }

        @Test
        @Timeout(value = 5)
        void shouldLoadSparseNodes() {
            runInTransaction(db, tx -> {
                tx.findNodes(Label.label("Label2"))
                    .stream().forEach(n -> {
                    long graphId = sparseGraph.nodes().toMappedNodeId(n.getId());
                    assertNotEquals(-1, graphId, n + " not mapped");
                    long neoId = sparseGraph.nodes().toOriginalNodeId(graphId);
                    assertEquals(n.getId(), neoId, n + " mapped wrongly");
                });
            });
        }
    }

    private boolean testRelationships(Transaction tx, Graph graph, long nodeId) {
        final Node node = getNodeById(tx, graph.toOriginalNodeId(nodeId));
        assertNotNull(node);
        final Map<Long, Relationship> relationships = StreamSupport
                .stream(node.getRelationships(Direction.OUTGOING).spliterator(), false)
                .collect(Collectors.toMap(
                        rel -> combineIntInt((int) rel.getStartNode().getId(), (int) rel.getEndNode().getId()),
                        Function.identity()));
        graph.forEachRelationship(
                nodeId,
                (sourceId, targetId) -> {
                    assertEquals(nodeId, sourceId);
                    long relId = combineIntInt((int) sourceId, (int) targetId);
                    final Relationship relationship = relationships.remove(relId);
                    assertNotNull(
                            relationship,
                            formatWithLocale(
                                    "Relationship (%d)-[%d]->(%d) that does not exist in the graph",
                                    sourceId,
                                    relId,
                                    targetId));

                    assertEquals(
                        relationship.getStartNode().getId(),
                        graph.toOriginalNodeId(sourceId)
                    );
                    assertEquals(
                        relationship.getEndNode().getId(),
                        graph.toOriginalNodeId(targetId));
                    return true;
                });

        assertTrue(
                relationships.isEmpty(),
                "Relationships that were not traversed " + relationships);

        return true;
    }

    private Graph loadEverything() {
        return load(db, l -> { }).getUnion();
    }

    private GraphStore load(GraphDatabaseAPI db, Consumer<StoreLoaderBuilder> block) {
        ExecutorService pool = Executors.newFixedThreadPool(3);
        StoreLoaderBuilder loader = new StoreLoaderBuilder()
            .databaseService(db)
            .executorService(pool);
        block.accept(loader);
        try {
            return loader.build().graphStore();
        } finally {
            pool.shutdown();
        }
    }

    private static final class ThrowingThreadPool extends ThreadPoolExecutor {
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
