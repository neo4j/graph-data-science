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
package org.neo4j.graphalgo.impl.betweenness;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphdb.Direction;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Note: Experimental
 * @author mknblch
 */
public class RandomDegreeSelectionStrategy implements RABrandesBetweennessCentrality.SelectionStrategy {

    private final Degrees degrees;
    private final Direction direction;
    private final double maxDegree;
    private final SimpleBitSet bitSet;
    private final int size;

    public RandomDegreeSelectionStrategy(Direction direction, Graph graph, ExecutorService pool, int concurrency) {
        this.degrees = graph;
        this.direction = direction;
        bitSet = new SimpleBitSet(Math.toIntExact(graph.nodeCount()));
        final SecureRandom random = new SecureRandom();
        final AtomicInteger mx = new AtomicInteger(0);
        ParallelUtil.iterateParallel(pool, Math.toIntExact(graph.nodeCount()), concurrency, node -> {
            final int degree = degrees.degree(node, direction);
            int current;
            do {
                current = mx.get();
            } while (degree > current && !mx.compareAndSet(current, degree));
        });
        maxDegree = mx.get();
        ParallelUtil.iterateParallel(pool, Math.toIntExact(graph.nodeCount()), concurrency, node -> {
            if (random.nextDouble() <= degrees.degree(node, direction) / maxDegree) {
                bitSet.put(node);
            }
        });
        size = bitSet.size();
    }

    @Override
    public boolean select(int nodeId) {
        return bitSet.contains(nodeId);
    }

    @Override
    public int size() {
        return size;
    }

}