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
package org.neo4j.gds.embeddings.graphsage.weighted;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.gds.embeddings.graphsage.NeighborhoodSampler;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.queue.BoundedLongPriorityQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class WeightedNeighborhoodSampler implements NeighborhoodSampler {
    private final double beta = 1d;
    private final Random random;

    public WeightedNeighborhoodSampler() {
        this.random = new Random();
    }

    public List<Long> sample(Graph graph, long nodeId, long numberOfSamples, long randomState) {
        AtomicLong remainingToSample = new AtomicLong(numberOfSamples);
        AtomicLong remainingToConsider = new AtomicLong(graph.degree(nodeId));
        List<Long> neighbors = new ArrayList<>();

        Pair<Double, Double> minMax = minMax(graph, nodeId);
        double min = minMax.getOne();
        double max = minMax.getTwo();

        graph.concurrentCopy().forEachRelationship(
            nodeId,
            1.0d,
            (source, target, weight) -> {
                if (remainingToSample.get() == 0 || remainingToConsider.get() == 0) {
                    return false;
                }

                double probability = (min == max) ?
                    randomDouble(randomState, source, target, graph.nodeCount()) :
                    (1.0 - Math.pow((weight - min) / (max - min), beta));

                if (remainingToConsider.getAndDecrement() * probability <= remainingToSample.get()) {
                    neighbors.add(target);
                    remainingToSample.decrementAndGet();
                }
                return true;
            }
        );

        return neighbors;
    }

    private double randomDouble(long randomState, long source, long target, long nodeCount) {
        random.setSeed(randomState + source + nodeCount * target);
        return random.nextDouble();
    }

    private Pair<Double, Double> minMax(Graph graph, long nodeId) {
        var maxQ = BoundedLongPriorityQueue.max(1);
        var minQ = BoundedLongPriorityQueue.min(1);
        graph.concurrentCopy().forEachRelationship(
            nodeId,
            1.0d,
            (source, target, weight) -> {
                maxQ.offer(target, weight);
                minQ.offer(target, weight);
                return true;
            }
        );

        var min = minQ.priorities().max().orElse(0D);
        var max = maxQ.priorities().min().orElse(0D);

        return Tuples.pair(min, max);
    }
}
