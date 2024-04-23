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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.collections.LongMultiSet;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertMemoryRange;
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
        var nodeIds = HugeLongArray.newArray(nodeIdsList.size());
        nodeIds.setAll(i -> nodeIdsList.get((int) i));

        // generate non-consecutive classes
        // then pick randomly from these
        var distinctTargets = new long[classCount];
        for (int i = 0; i < classCount; i++) {
            distinctTargets[i] = rnd.nextLong();
        }
        var targets = HugeLongArray.newArray(nodeCount);
        targets.setAll(i -> {
            var classOffset = rnd.nextInt(classCount);
            return distinctTargets[classOffset];
        });
        var totalClassCounts = classCounts(targets);

        var kFoldSplitter = new StratifiedKFoldSplitter(
            k,
            ReadOnlyHugeLongArray.of(nodeIds),
            targets::get,
            Optional.of(42L),
            new TreeSet<>(Arrays.stream(distinctTargets).boxed().collect(Collectors.toList()))
        );
        var splits = kFoldSplitter.splits();
        assertThat(splits.size()).isEqualTo(k);
        var unionOfTestSets = new HashSet<Long>();
        for (int fold = 0; fold < k; fold++) {
            var split = splits.get(fold);
            var trainSet = split.trainSet();
            var testSet = split.testSet();
            var nodeSet = new HashSet<Long>();
            Arrays.stream(trainSet.toArray()).forEach(nodeSet::add);
            Arrays.stream(testSet.toArray()).forEach(testId -> {
                nodeSet.add(testId);
                unionOfTestSets.add(testId);
            });

            assertThat(nodeSet.size()).isEqualTo(nodeCount);
            // (k-1) * nodeCount/k - trainSet.size() should be between -1 and 1
            // multiply both sides by k
            assertThat( Math.abs((k-1) * nodeCount - k * trainSet.size())).isLessThanOrEqualTo(k);
            // nodeCount/k - testSet.size() should be between -1 and 1
            assertThat( Math.abs(nodeCount - k * testSet.size())).isLessThanOrEqualTo(k);

            // check that stratification works: class proportions approximately preserved
            // in test and train sets
            var testClassCounts = classCountsForSet(testSet, targets);
            var trainClassCounts = classCountsForSet(trainSet, targets);

            Arrays.stream(distinctTargets).forEach(
                target -> {
                    var totalOccurrences = totalClassCounts.count(target);
                    var trainOccurrences = trainClassCounts.count(target);
                    var lowerExpectedTrain = (k - 1) * totalOccurrences / k;
                    var upperExpectedTrain = lowerExpectedTrain + 1;

                    assertThat(trainOccurrences).isLessThanOrEqualTo(upperExpectedTrain);
                    assertThat(trainOccurrences).isGreaterThanOrEqualTo(lowerExpectedTrain);

                    var testOccurrences = testClassCounts.count(target);
                    var lowerExpectedTest = totalOccurrences / k;
                    var upperExpectedTest = lowerExpectedTest + 1;
                    assertThat(testOccurrences).isLessThanOrEqualTo(upperExpectedTest);
                    assertThat(testOccurrences).isGreaterThanOrEqualTo(lowerExpectedTest);
                });
        }
        assertThat(unionOfTestSets.size()).isEqualTo(nodeCount);
    }

    @Test
    void minAndMaxEstimationsAreTheSame() {
        var estimation = StratifiedKFoldSplitter.memoryEstimationForNodeSet(5, 0.1)
            .estimate(GraphDimensions.of(1000), new Concurrency(1))
            .memoryUsage();
        assertThat(estimation.min).isEqualTo(estimation.max);
    }

    @ParameterizedTest
    @CsvSource(value = {
        " 1_000,  4, 0.5,  16_320",
        // x4 number of folds same factor in estimation
        " 1_000, 16, 0.5,  65_280",
        // 1/5 trainFraction -> 1/5 train-set size and resulting folds
        " 1_000,  4, 0.1,   3_520",
        // x10 nodeCount same factor in estimation
        "10_000,  4, 0.5, 160_320",
    })
    void memoryEstimationShouldScaleWithNumberOfFolds(long nodeCount, int k, double trainFraction, long expectedMemory) {
        var dimensions = GraphDimensions.of(nodeCount);
        var actualEstimation = StratifiedKFoldSplitter.memoryEstimationForNodeSet(k, trainFraction)
            .estimate(dimensions, new Concurrency(4))
            .memoryUsage();

        assertMemoryRange(actualEstimation, expectedMemory);
    }

    private LongMultiSet classCounts(HugeLongArray values) {
        var counts = new LongMultiSet();
        Arrays.stream(values.toArray()).forEach(counts::add);

        return counts;
    }

    private LongMultiSet classCountsForSet(ReadOnlyHugeLongArray idSet, HugeLongArray targets) {
        var counts = new LongMultiSet();

        for (long i = 0; i < idSet.size(); i++) {
            counts.add(targets.get(idSet.get(i)));
        }

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
