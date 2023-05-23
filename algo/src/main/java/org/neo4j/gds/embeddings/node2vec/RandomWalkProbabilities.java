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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.stream.LongStream;

import static java.lang.Math.addExact;

@ValueClass
interface RandomWalkProbabilities {

    HugeLongArray nodeFrequencies();
    HugeDoubleArray positiveSamplingProbabilities();
    HugeLongArray negativeSamplingDistribution();
    long sampleCount();

    static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(RandomWalkProbabilities.class.getSimpleName())
            .perNode("node frequencies", HugeLongArray::memoryEstimation)
            .perNode("positive sampling probabilities", HugeDoubleArray::memoryEstimation)
            .perNode("negative sampling distribution", HugeLongArray::memoryEstimation)
            .build();
    }

    @SuppressWarnings("immutables:incompat")
    class Builder {

        private final long nodeCount;
        private final int concurrency;
        private final double positiveSamplingFactor;
        private final double negativeSamplingExponent;
        private final HugeLongArray nodeFrequencies;
        private final MutableLong sampleCount;

        Builder(
            long nodeCount,
            double positiveSamplingFactor,
            double negativeSamplingExponent,
            int concurrency
        ) {
            this.nodeCount = nodeCount;
            this.concurrency = concurrency;
            this.positiveSamplingFactor = positiveSamplingFactor;
            this.negativeSamplingExponent = negativeSamplingExponent;

            this.nodeFrequencies = HugeLongArray.newArray(nodeCount);
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
            var centerProbabilities = computePositiveSamplingProbabilities();
            var contextDistribution = computeNegativeSamplingDistribution();

            return ImmutableRandomWalkProbabilities
                .builder()
                .nodeFrequencies(nodeFrequencies)
                .positiveSamplingProbabilities(centerProbabilities)
                .negativeSamplingDistribution(contextDistribution)
                .sampleCount(sampleCount.getValue())
                .build();
        }

        private HugeDoubleArray computePositiveSamplingProbabilities() {
            var centerProbabilities = HugeDoubleArray.newArray(nodeCount);
            var sum = sampleCount.getValue();

            ParallelUtil.parallelStreamConsume(
                LongStream.range(0, nodeCount),
                concurrency,
                TerminationFlag.RUNNING_TRUE,
                nodeStream -> nodeStream.forEach(nodeId -> {
                        double frequency = ((double) nodeFrequencies.get(nodeId)) / sum;
                        centerProbabilities.set(
                            nodeId,
                            (Math.sqrt(frequency / positiveSamplingFactor) + 1) * (positiveSamplingFactor / frequency)
                        );
                    })
            );

            return centerProbabilities;
        }

        private HugeLongArray computeNegativeSamplingDistribution() {
            var contextDistribution = HugeLongArray.newArray(nodeCount);
            long sum = 0;
            for (var i = 0L; i < nodeCount; i++) {
                sum += Math.pow(nodeFrequencies.get(i), negativeSamplingExponent);
                sum = addExact(sum, (long) Math.pow(nodeFrequencies.get(i), negativeSamplingExponent));
                contextDistribution.set(i, sum);
            }

            return contextDistribution;
        }
    }

}
