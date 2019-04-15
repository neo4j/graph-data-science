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
package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.container.AtomicBitSet;
import org.neo4j.graphalgo.core.utils.container.FlipStack;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.IntPredicate;

/**
 * Multistep SCC coloring algorithm.
 * <p>
 * The algorithm assigns a color to each node. The color always reflects
 * the highest node id in the set. Initially all nodes are colored using their
 * own nodeId. The algorithm itself builds weakly connected components
 * which are then merged with its predecessor set to get a SCC.
 * <p>
 * More Info:
 * <p>
 * http://www.sandia.gov/~srajama/publications/BFS_and_Coloring.pdf
 * https://www.osti.gov/scitech/servlets/purl/1115145
 *
 * @author mknblch
 */
public class MultiStepColoring {

    public static final int MIN_BATCH_SIZE = 100_000;

    private final Graph graph;
    private final ExecutorService executorService;
    private final AtomicIntegerArray colors;
    private final AtomicBitSet visited;
    private final List<Future<IntContainer>> futures = new ArrayList<>();
    private final int concurrency;
    private final int nodeCount;

    public MultiStepColoring(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        colors = new AtomicIntegerArray(nodeCount);
        visited = new AtomicBitSet(nodeCount);
    }

    /**
     * compute the colors of nodes
     *
     * @param nodes set of nodes
     * @return self for method chaining
     */
    public MultiStepColoring compute(IntSet nodes) {
        resetColors(nodes);
        msColorParallel(nodes);
        return this;
    }

    public AtomicIntegerArray getColors() {
        return colors;
    }

    /**
     * for each distinct color
     *
     * @param consumer color consumer
     */
    public void forEachColor(IntPredicate consumer) {
        final SimpleBitSet bitSet = new SimpleBitSet(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            final int color = colors.get(i);
            if (!bitSet.contains(color)) {
                bitSet.put(color);
                if (!consumer.test(color)) {
                    return;
                }
            }
        }
    }

    /**
     * parallel multistep coloring algorithm
     *
     * @param nodeSet
     */
    private void msColorParallel(IntSet nodeSet) {

        final FlipStack flipStack = new FlipStack(nodeSet);

        flipStack.flip();

        while (!flipStack.isEmpty()) {
            // reset result futures
            futures.clear();
            // calculate batch size
            final int size = flipStack.popStack().size();
            final int batchSize = Math.floorDiv(size, concurrency);
            // no need for parallel exec.
            if (concurrency <= 1 || batchSize < MIN_BATCH_SIZE) {
                futures.add(executorService.submit(() -> msColorTask(flipStack.popStack())));
            } else {
                // split the actual nodeSet into batches
                final Iterator<IntCursor> it = flipStack.popStack().iterator();
                for (int i = 0; i < size; i += batchSize) {
                    // creating partition must happen sequential
                    final IntScatterSet partition = partition(it, batchSize);
                    // enqueue partition
                    futures.add(executorService.submit(() -> msColorTask(partition)));
                }
            }

            // sync all results into the current pushStack
            flipStack.pushStack().clear();
            union(flipStack.pushStack(), futures);
            // flip the stack so pushStack becomes popStack and vice versa
            flipStack.flip();
        }
    }

    /**
     * multistep coloring algorithm task
     *
     * @param nodes nodes to process
     * @return container with nodes which must be processed in the next step
     */
    private IntContainer msColorTask(IntContainer nodes) {

        final IntStack levelQueue = new IntStack(nodes.size());

        nodes.forEach((IntProcedure) node -> {
            final int nodeColor = colors.get(node);
            final boolean[] change = {false};
            graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                if (cas(targetNodeId, nodeColor)) {
                    if (!visited.get(targetNodeId)) {
                        visited.set(targetNodeId);
                        levelQueue.push(targetNodeId);
                        change[0] = true;
                    }
                }
                return true;
            });
            if (change[0] && !visited.get(node)) {
                levelQueue.push(node);
                visited.set(node);
            }
        });

        return levelQueue;
    }

    /**
     * sequential impl. based on a flippable stack
     *
     * @param nodes
     */
    private void msColorSequential(IntSet nodes) {

        final FlipStack queue = new FlipStack(nodes.size());
        queue.addAll(nodes);
        queue.flip();

        while (!queue.isEmpty()) {
            queue.forEach(node -> {
                final int nodeColor = colors.get(node);
                final boolean[] change = {false};
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    if (cas(targetNodeId, nodeColor)) {
                        if (!visited.get(targetNodeId)) {
                            visited.set(targetNodeId);
                            queue.push(targetNodeId);
                            change[0] = true;
                        }
                    }
                    return true;
                });
                if (change[0] && !visited.get(node)) {
                    queue.push(node);
                    visited.set(node);
                }
            });
            queue.popStack().clear();
            queue.flip();
        }

    }

    /**
     * simple coloring impl.
     *
     * @param nodes
     */
    private void simpleColor(IntSet nodes) {

        final boolean[] changed = {false};
        do {
            changed[0] = false;
            nodes.forEach((IntProcedure) node -> {
                final int nodeColor = colors.get(node);
                // for each <v, u> in E(V) (direction not specified)
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    if (cas(targetNodeId, nodeColor)) {
                        changed[0] = true;
                    }
                    return true;
                });
            });

        } while (changed[0]);
    }

    private void resetColors(IntContainer nodes) {
        nodes.forEach((IntProcedure) node -> colors.set(node, node));
    }

    /**
     * extracts a node partition
     *
     * @param it        the node iterator
     * @param batchSize the maximum batch size
     * @return a batch of nodes with elements <= batchSize
     */
    private IntScatterSet partition(Iterator<IntCursor> it, int batchSize) {
        final IntScatterSet partition = new IntScatterSet(batchSize);
        for (int j = 0; j < batchSize && it.hasNext(); j++) {
            partition.add(it.next().value);
        }
        return partition;
    }

    /**
     * compare and set color only if the new color
     * is greater then the existing
     *
     * @param nodeId the node id
     * @param color  the color
     * @return true if color was assigned, false otherwise
     */
    private boolean cas(int nodeId, int color) {
        boolean stored = false;
        while (!stored) {
            int oldC = colors.get(nodeId);
            if (color > oldC) {
                stored = colors.compareAndSet(nodeId, oldC, color);
            } else {
                break;
            }
        }
        return stored;
    }

    /**
     * union all IntContainer from futures into ret set
     *
     * @param ret     result set
     * @param futures
     * @return
     */
    private void union(IntStack ret, Collection<Future<IntContainer>> futures) {
        boolean done = false;
        Throwable error = null;
        try {
            for (Future<IntContainer> future : futures) {
                try {
                    future.get().forEach((IntProcedure) ret::add);
                } catch (ExecutionException ee) {
                    error = Exceptions.chain(error, ee.getCause());
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
    }

}
