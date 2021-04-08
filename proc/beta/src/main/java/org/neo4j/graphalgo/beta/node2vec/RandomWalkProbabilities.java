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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.stream.LongStream;

import static java.lang.Math.addExact;

@ValueClass
interface RandomWalkProbabilities {

    HugeLongArray nodeFrequencies();
    HugeDoubleArray centerProbabilities();
    HugeLongArray contextDistribution();

    static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(RandomWalkProbabilities.class)
            .perNode("node frequencies", HugeLongArray::memoryEstimation)
            .perNode("center probabilities", HugeDoubleArray::memoryEstimation)
            .perNode("context distribution", HugeLongArray::memoryEstimation)
            .build();
    }

    class Builder {

        private final long nodeCount;
        private final int concurrency;
        private final AllocationTracker tracker;
        private final double centerSamplingFactor;
        private final double contextSamplingExponent;
        private final HugeLongArray nodeFrequencies;
        private final MutableLong sampleCount;

        Builder(
            long nodeCount,
            double centerSamplingFactor, double contextSamplingExponent, int concurrency,
            AllocationTracker tracker
        ) {
            this.nodeCount = nodeCount;
            this.concurrency = concurrency;
            this.tracker = tracker;
            this.centerSamplingFactor = centerSamplingFactor;
            this.contextSamplingExponent = contextSamplingExponent;

            this.nodeFrequencies = HugeLongArray.newArray(nodeCount, tracker);
            this.sampleCount = new MutableLong(0);
        }

        RandomWalkProbabilities.Builder registerWalk(long[] walk) {
            for (long node : walk) {
                nodeFrequencies.addTo(node, 1);
            }
            this.sampleCount.add(walk.length);

            return this;
        }

        RandomWalkProbabilities build() {
            var centerProbabilities = computeCenterProbabilities();
            var contextDistribution = computeContextDistribution();

            return ImmutableRandomWalkProbabilities
                .builder()
                .nodeFrequencies(nodeFrequencies)
                .centerProbabilities(centerProbabilities)
                .contextDistribution(contextDistribution)
                .build();
        }

        private HugeDoubleArray computeCenterProbabilities() {
            var centerProbabilities = HugeDoubleArray.newArray(nodeCount, tracker);
            var sum = sampleCount.getValue();

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

            return centerProbabilities;
        }

        private HugeLongArray computeContextDistribution() {
            var contextDistribution = HugeLongArray.newArray(nodeCount, tracker);
            long sum = 0;
            for (var i = 0L; i < nodeCount; i++) {
                sum += Math.pow(nodeFrequencies.get(i), contextSamplingExponent);
                sum = addExact(sum, (long) Math.pow(nodeFrequencies.get(i), contextSamplingExponent));
                contextDistribution.set(i, sum);
            }

            return contextDistribution;
        }
    }

}
