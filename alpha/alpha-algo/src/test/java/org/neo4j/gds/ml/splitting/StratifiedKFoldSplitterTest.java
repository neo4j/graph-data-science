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
package org.neo4j.gds.ml.splitting;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.crossArguments;

class StratifiedKFoldSplitterTest {

    @ParameterizedTest
    @MethodSource("splitArguments")
    void shouldGiveCorrectSplits(long nodeCount, long seed, int k, int classCount) {
        // shuffled consecutive ids
        Random rnd = new Random(seed);
        var nodeIdsList = LongStream.range(0, nodeCount).boxed()
            .collect(Collectors.toList());
        Collections.shuffle(nodeIdsList, rnd);
        var nodeIds = HugeLongArray.newArray(nodeIdsList.size(), AllocationTracker.empty());
        nodeIds.setAll(i -> nodeIdsList.get((int) i));

        // generate non-consecutive classes
        // then pick randomly from these
        var distinctTargets = new long[classCount];
        for (int i = 0; i < classCount; i++) {
            distinctTargets[i] = rnd.nextLong();
        }
        var targets = HugeLongArray.newArray(nodeCount, AllocationTracker.empty());
        targets.setAll(i -> {
            var classOffset = rnd.nextInt(classCount);
            return distinctTargets[classOffset];
        });
        var totalClassCounts = classCounts(targets);


        var kFoldSplitter = new StratifiedKFoldSplitter(k, nodeIds, targets, Optional.of(42L));
        var splits = kFoldSplitter.splits();
        assertThat(splits.size()).isEqualTo(k);
        var unionOfTestSets = new HashSet<Long>();
        for (int fold = 0; fold < k; fold++) {
            var split = splits.get(fold);
            var trainSet = split.trainSet();
            var testSet = split.testSet();
            var nodeSet = new HashSet<Long>();
            stream(trainSet.toArray()).forEach(nodeSet::add);
            stream(testSet.toArray()).forEach(nodeSet::add);
            stream(testSet.toArray()).forEach(unionOfTestSets::add);
            assertThat(nodeSet.size()).isEqualTo(nodeCount);
            // (k-1) * nodeCount/k - trainSet.size() should be between -1 and 1
            // multiply both sides by k
            assertThat( Math.abs((k-1) * nodeCount - k * trainSet.size())).isLessThanOrEqualTo(k);
            // nodeCount/k - testSet.size() should be between -1 and 1
            assertThat( Math.abs(nodeCount - k * testSet.size())).isLessThanOrEqualTo(k);

            // check that stratification works: class proportions approximately preserved
            // in test and train sets
            var testClassCounts = classCounts(testSet);
            var trainClassCounts = classCounts(trainSet);
            stream(distinctTargets).forEach(
                target -> {
                    var totalOccurances = totalClassCounts.getOrDefault(target, 0);
                    var trainOccurances = trainClassCounts.getOrDefault(target, 0);
                    var lowerExpectedTrain = (k - 1) * totalOccurances / k;
                    var upperExpectedTrain = lowerExpectedTrain + 1;
                    assertThat(trainOccurances).isLessThanOrEqualTo(upperExpectedTrain);
                    assertThat(trainOccurances).isGreaterThanOrEqualTo(lowerExpectedTrain);
                    var testOccurances = testClassCounts.getOrDefault(target, 0);
                    var lowerExpectedTest = totalOccurances / k;
                    var upperExpectedTest = lowerExpectedTest + 1;
                    assertThat(testOccurances).isLessThanOrEqualTo(upperExpectedTest);
                    assertThat(testOccurances).isGreaterThanOrEqualTo(lowerExpectedTest);
                });
        }
        assertThat(unionOfTestSets.size()).isEqualTo(nodeCount);
    }

    @Test
    void minAndMaxEstimationsAreTheSame() {
        var estimation = StratifiedKFoldSplitter.memoryEstimation(5, 0.1)
            .estimate(GraphDimensions.of(1000), 1)
            .memoryUsage();
        assertThat(estimation.min).isEqualTo(estimation.max);
    }

    @Test
    void memoryEstimationShouldScaleWithNumberOfFolds() {
        var dimensions = GraphDimensions.of(1000);
        var memoryUsageForTenFolds = StratifiedKFoldSplitter.memoryEstimation(10, 0.1)
            .estimate(dimensions, 1)
            .memoryUsage();
        var memoryUsageForFiveFolds = StratifiedKFoldSplitter.memoryEstimation(5, 0.1)
            .estimate(dimensions, 1)
            .memoryUsage();
        assertThat(memoryUsageForTenFolds.min).isCloseTo(2 * memoryUsageForFiveFolds.min, Offset.offset(100L));
        assertThat(memoryUsageForTenFolds.max).isCloseTo(2 * memoryUsageForFiveFolds.max, Offset.offset(100L));
    }

    private Map<Long, Integer> classCounts(HugeLongArray values) {
        // would be nice to have MultiSet or Counter class
        var counts = new HashMap<Long, Integer>();
        stream(values.toArray())
            .forEach(value ->
                counts.put(value, counts.getOrDefault(counts, 0) + 1)
            );
        return counts;
    }

    private static Stream<Arguments> splitArguments() {
        return crossArguments(
            // nodeCount
            () -> Stream.of(0, 1, 10, 100).map(Arguments::of),
            // seed
            () -> LongStream.range(0L, 10L).mapToObj(Arguments::of),
            // k
            () -> IntStream.range(2, 10).mapToObj(Arguments::of),
            // classCount
            () -> IntStream.range(1, 4).mapToObj(Arguments::of)
        );
    }
}
