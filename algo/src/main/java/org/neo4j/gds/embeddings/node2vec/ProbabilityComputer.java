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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.stream.LongStream;

import static java.lang.Math.addExact;

public class ProbabilityComputer {

    private final HugeObjectArray<long[]> walks;
    private final long nodeCount;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final double centerSamplingFactor;
    private final double contextSamplingExponent;

    private HugeAtomicLongArray nodeFrequencies;
    private HugeDoubleArray centerProbabilities;
    private HugeLongArray contextDistribution;

    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(ProbabilityComputer.class)
            .perNode("node frequencies", HugeAtomicLongArray::memoryEstimation)
            .perNode("center probabilities", HugeDoubleArray::memoryEstimation)
            .perNode("context distribution", HugeLongArray::memoryEstimation)
            .build();
    }

    public ProbabilityComputer(
        HugeObjectArray<long[]> walks,
        long nodeCount,
        double centerSamplingFactor,
        double contextSamplingExponent,
        int concurrency,
        AllocationTracker tracker
    ) {
        this.walks = walks;
        this.nodeCount = nodeCount;
        this.concurrency = concurrency;
        this.tracker = tracker;
        this.centerSamplingFactor = centerSamplingFactor;
        this.contextSamplingExponent = contextSamplingExponent;

        computeFrequencies();
        computeCenterProbabilities();
        computeContextDistribution();
    }

    HugeDoubleArray getCenterNodeProbabilities() {
        return centerProbabilities;
    }

    HugeLongArray getContextNodeDistribution() {
        return contextDistribution;
    }

    private void computeFrequencies() {
        nodeFrequencies = HugeAtomicLongArray.newArray(nodeCount, tracker);
        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, walks.size()),
            concurrency,
            walkIdStream -> walkIdStream.forEach(walkId -> {
                var walk = walks.get(walkId);
                for (long node : walk) {
                    nodeFrequencies.update(node, count -> addExact(count, 1));
                }
            })
        );
    }

    private void computeCenterProbabilities() {
        var sum = ParallelUtil.parallelStream(
            LongStream.range(0, nodeCount),
            concurrency,
            nodeIdStream -> nodeIdStream.map(nodeFrequencies::get).reduce(0, Math::addExact)
        );

        centerProbabilities = HugeDoubleArray.newArray(nodeCount, tracker);
        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, nodeCount),
            concurrency,
            nodeStream -> nodeStream.forEach(nodeId -> {
                double frequency = ((double) nodeFrequencies.get(nodeId)) / sum;
                centerProbabilities.set(
                    nodeId,
                    (Math.sqrt(frequency / centerSamplingFactor) + 1) * (centerSamplingFactor / frequency)
                );
            })
        );
    }

    private void computeContextDistribution() {
        contextDistribution = HugeLongArray.newArray(nodeCount, tracker);
        long sum = 0;
        for (var i = 0L; i < nodeCount; i++) {
            sum += Math.pow(nodeFrequencies.get(i), contextSamplingExponent);
            sum = addExact(sum, (long) Math.pow(nodeFrequencies.get(i), contextSamplingExponent));
            contextDistribution.set(i, sum);
        }
    }

}
