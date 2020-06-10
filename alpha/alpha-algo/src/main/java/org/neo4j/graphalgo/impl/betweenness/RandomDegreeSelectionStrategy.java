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
package org.neo4j.graphalgo.impl.betweenness;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedSimpleBitSet;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Note: Experimental
 */
public class RandomDegreeSelectionStrategy implements RABrandesBetweennessCentrality.SelectionStrategy {

    private final double maxDegree;
    // TODO: benchmark and potentially replace with hppc BitSet
    private final PagedSimpleBitSet bitSet;
    private final long size;

    public RandomDegreeSelectionStrategy(
        Graph graph,
        double probabilityOffset,
        ExecutorService executorService,
        int concurrency,
        AllocationTracker tracker
    ) {
        this.bitSet = PagedSimpleBitSet.newBitSet(graph.nodeCount(), tracker);
        this.maxDegree = getMaxDegree(graph, executorService, concurrency);

        SecureRandom random = new SecureRandom();
        ParallelUtil.iterateParallel(executorService, Math.toIntExact(graph.nodeCount()), concurrency, node -> {
            if (random.nextDouble() - probabilityOffset <= graph.degree(node) / maxDegree) {
                bitSet.put(node);
            }
        });

        this.size = bitSet.size();
    }

    @Override
    public boolean select(long nodeId) {
        return bitSet.contains(nodeId);
    }

    @Override
    public long size() {
        return size;
    }

    private long getMaxDegree(Graph graph, ExecutorService pool, int concurrency) {
        AtomicInteger mx = new AtomicInteger(0);
        ParallelUtil.iterateParallel(pool, Math.toIntExact(graph.nodeCount()), concurrency, node -> {
            int degree = graph.degree(node);
            int current;
            do {
                current = mx.get();
            } while (degree > current && !mx.compareAndSet(current, degree));
        });
        return mx.get();
    }
}
