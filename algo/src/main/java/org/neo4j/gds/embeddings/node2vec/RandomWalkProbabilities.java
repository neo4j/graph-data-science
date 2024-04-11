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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.atomic.LongAdder;
import java.util.stream.LongStream;

import static java.lang.Math.addExact;

@ValueClass
interface RandomWalkProbabilities {

    HugeAtomicLongArray nodeFrequencies();
    HugeDoubleArray positiveSamplingProbabilities();
    HugeLongArray negativeSamplingDistribution();
    long sampleCount();

    @SuppressWarnings("immutables:incompat")
    class Builder {

        private final long nodeCount;
        private final Concurrency concurrency;
        private final double positiveSamplingFactor;
        private final double negativeSamplingExponent;
        private final HugeAtomicLongArray nodeFrequencies;
        private final LongAdder sampleCount;

        Builder(
            long nodeCount,
            Concurrency concurrency,
            double positiveSamplingFactor,
            double negativeSamplingExponent
        ) {
            this.nodeCount = nodeCount;
            this.concurrency = concurrency;
            this.positiveSamplingFactor = positiveSamplingFactor;
            this.negativeSamplingExponent = negativeSamplingExponent;

            this.nodeFrequencies = HugeAtomicLongArray.of(nodeCount, ParalleLongPageCreator.passThrough(concurrency.value()));
            this.sampleCount = new LongAdder();
        }

        //wip to break for the day
        void registerWalk(long[] walk) {
            for (long node : walk) {
                nodeFrequencies.getAndAdd(node, 1);
            }
            this.sampleCount.add(walk.length);

        }

        RandomWalkProbabilities build() {
            var centerProbabilities = computePositiveSamplingProbabilities();
            var contextDistribution = computeNegativeSamplingDistribution();

            return ImmutableRandomWalkProbabilities
                .builder()
                .nodeFrequencies(nodeFrequencies)
                .positiveSamplingProbabilities(centerProbabilities)
                .negativeSamplingDistribution(contextDistribution)
                .sampleCount(sampleCount.longValue())
                .build();
        }

        private HugeDoubleArray computePositiveSamplingProbabilities() {
            var centerProbabilities = HugeDoubleArray.newArray(nodeCount);
            var sum = sampleCount.longValue();

            ParallelUtil.parallelStreamConsume(
                LongStream.range(0, nodeCount),
                concurrency.value(),
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
