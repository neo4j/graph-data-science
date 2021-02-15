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

import org.neo4j.graphalgo.api.Graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class UniformNeighborhoodSampler implements NeighborhoodSampler {
    private final Random random;
    private long randomSeed;

    public UniformNeighborhoodSampler(long randomSeed) {
        this.random = new Random(randomSeed);
        this.randomSeed = randomSeed;
    }

    public List<Long> sample(Graph graph, long nodeId, long numberOfSamples) {
        AtomicLong remainingToSample = new AtomicLong(numberOfSamples);
        AtomicLong remainingToConsider = new AtomicLong(graph.degree(nodeId));
        List<Long> neighbors = new ArrayList<>();
        graph.concurrentCopy().forEachRelationship(
            nodeId,
            (source, target) -> {
                double randomDouble = randomDouble(source, target, graph.nodeCount());
                if (remainingToConsider.getAndDecrement() * randomDouble <= remainingToSample.get()) {
                    neighbors.add(target);
                    remainingToSample.decrementAndGet();
                }
                return remainingToSample.get() != 0 && remainingToConsider.get() != 0;
            }
        );
        return neighbors;
    }

    private double randomDouble(long source, long target, long nodeCount) {
        random.setSeed(randomSeed + source + nodeCount * target);
        return random.nextDouble();
    }

    @Override
    public long randomState() {
        return this.randomSeed;
    }

    @Override
    public void generateNewRandomState() {
        this.randomSeed = ThreadLocalRandom.current().nextLong();
    }
}
