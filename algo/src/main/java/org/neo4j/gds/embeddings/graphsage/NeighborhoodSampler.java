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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.ml.core.RelationshipWeights;
import org.neo4j.gds.ml.core.batch.UniformReservoirLSampler;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.queue.BoundedLongPriorityQueue;

import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

public class NeighborhoodSampler {
    // Influence of the weight for the probability
    private final double beta = 1D;
    private long randomSeed;

    public NeighborhoodSampler(long randomSeed) {
        this.randomSeed = randomSeed;
    }

    public LongStream sample(Graph graph, long nodeId, long numberOfSamples) {
        var degree = graph.degree(nodeId);

        // Every neighbor needs to be sampled
        if (degree <= numberOfSamples) {
            return graph.concurrentCopy()
                .streamRelationships(nodeId, RelationshipWeights.DEFAULT_VALUE)
                .mapToLong(RelationshipCursor::targetId);
        }

        var minMax = minMax(graph, nodeId);
        double min = minMax.min();
        double max = minMax.max();

        // If the weights are all the same trigger the unweighted case.
        if (min == max) {
            var neighbours = graph
                .concurrentCopy()
                .streamRelationships(nodeId, RelationshipWeights.DEFAULT_VALUE)
                .mapToLong(RelationshipCursor::targetId);
            return new UniformReservoirLSampler(randomSeed).sample(
                neighbours,
                graph.degree(nodeId),
                Math.toIntExact(numberOfSamples)
            );
        } else {
            return sampleWeighted(graph.concurrentCopy(), nodeId, degree, numberOfSamples, min, max);
        }
    }

    long randomState() {
        return this.randomSeed;
    }

    void generateNewRandomState() {
        this.randomSeed = ThreadLocalRandom.current().nextLong();
    }

    OptionalLong sampleOne(Graph graph, long nodeId) {
        return sample(graph, nodeId, 1).findFirst();
    }

    private LongStream sampleWeighted(
        Graph graph,
        long nodeId,
        int degree,
        long numberOfSamples,
        double min,
        double max
    ) {

        var remainingToSample = new MutableLong(numberOfSamples);
        var remainingToConsider = new MutableLong(degree);
        var neighbors = LongStream.builder();

        double maxMinDiff = (max - min);

        graph.forEachRelationship(
            nodeId,
            RelationshipWeights.DEFAULT_VALUE,
            (source, target, weight) -> {
                if (remainingToSample.longValue() == 0 || remainingToConsider.longValue() == 0) {
                    return false;
                }

                double probability = (1.0 - Math.pow((weight - min) / maxMinDiff, beta));
                if (remainingToConsider.getAndDecrement() * probability <= remainingToSample.longValue()) {
                    neighbors.add(target);
                    remainingToSample.decrementAndGet();
                }
                return true;
            }
        );
        return neighbors.build();
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
