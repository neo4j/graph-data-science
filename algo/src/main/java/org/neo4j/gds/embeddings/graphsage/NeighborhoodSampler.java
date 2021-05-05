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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.ml.core.RelationshipWeights;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.queue.BoundedLongPriorityQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class NeighborhoodSampler {
    private final double beta = 1D;
    private final Random random;
    private long randomSeed;

    public NeighborhoodSampler(long randomSeed) {
        this.randomSeed = randomSeed;
        this.random = new Random(randomSeed);
    }

    public List<Long> sample(Graph graph, long nodeId, long numberOfSamples) {
        AtomicLong remainingToSample = new AtomicLong(numberOfSamples);
        AtomicLong remainingToConsider = new AtomicLong(graph.degree(nodeId));
        List<Long> neighbors = new ArrayList<>();

        var minMax = minMax(graph, nodeId);
        double min = minMax.min();
        double max = minMax.max();

        if (min == max) {
            graph.concurrentCopy().forEachRelationship(
                nodeId,
                (source, target) -> {
                    if (remainingToSample.get() == 0 || remainingToConsider.get() == 0) {
                        return false;
                    }
                    double probability = randomDouble(source, target, graph.nodeCount());
                    if (remainingToConsider.getAndDecrement() * probability <= remainingToSample.get()) {
                        neighbors.add(target);
                        remainingToSample.decrementAndGet();
                    }
                    return true;
                }
            );
        } else {
            graph.concurrentCopy().forEachRelationship(
                nodeId,
                RelationshipWeights.DEFAULT_VALUE,
                (source, target, weight) -> {
                    if (remainingToSample.get() == 0 || remainingToConsider.get() == 0) {
                        return false;
                    }

                    double probability = (1.0 - Math.pow((weight - min) / (max - min), beta));

                    if (remainingToConsider.getAndDecrement() * probability <= remainingToSample.get()) {
                        neighbors.add(target);
                        remainingToSample.decrementAndGet();
                    }
                    return true;
                }
            );
        }

        graph.concurrentCopy().forEachRelationship(
            nodeId,
            RelationshipWeights.DEFAULT_VALUE,
            (source, target, weight) -> {
                if (remainingToSample.get() == 0 || remainingToConsider.get() == 0) {
                    return false;
                }

                double probability = (min == max) ?
                    randomDouble(source, target, graph.nodeCount()) :
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

    private double randomDouble(long source, long target, long nodeCount) {
        random.setSeed(this.randomSeed + source + nodeCount * target);
        return random.nextDouble();
    }

    public long randomState() {
        return this.randomSeed;
    }

    public void generateNewRandomState() {
        this.randomSeed = ThreadLocalRandom.current().nextLong();
    }

    public OptionalLong sampleOne(Graph graph, long nodeId) {
        List<Long> samples = sample(graph, nodeId, 1);
        if (samples.size() < 1) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(samples.get(0));
    }

    private MinMax minMax(Graph graph, long nodeId) {
        var maxQ = BoundedLongPriorityQueue.max(1);
        var minQ = BoundedLongPriorityQueue.min(1);

        if(!graph.hasRelationshipProperty()) {
            return MinMax.of(0D, 0D);
        }

        graph.concurrentCopy().forEachRelationship(
            nodeId,
            RelationshipWeights.DEFAULT_VALUE,
            (source, target, weight) -> {
                maxQ.offer(target, weight);
                minQ.offer(target, weight);
                return true;
            }
        );

        var min = minQ.priorities().max().orElse(0D);
        var max = maxQ.priorities().min().orElse(0D);

        return MinMax.of(min, max);
    }

    @ValueClass
    interface MinMax {
        double min();
        double max();

        static MinMax of(double min, double max) {
            return ImmutableMinMax.of(min, max);
        }
    }
}
