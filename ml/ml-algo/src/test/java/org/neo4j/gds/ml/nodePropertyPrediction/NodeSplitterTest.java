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
package org.neo4j.gds.ml.nodePropertyPrediction;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.InspectableTestProgressTracker;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class NodeSplitterTest {

    @Test
    void testSplitter() {
        int numberOfExamples = 12;
        var splitter = new NodeSplitter(
            new Concurrency(4),
            numberOfExamples,
            ProgressTracker.NULL_TRACKER,
            i -> i,
            i -> i
        );

        double testFraction = 0.25;
        int validationFolds = 3;
        var splits = splitter.split(testFraction, validationFolds, Optional.of(42L));

        assertThat(splits.allTrainingExamples().toArray())
            .hasSize(numberOfExamples)
            .containsExactlyInAnyOrder(LongStream.range(0, numberOfExamples).toArray())
            .matches(ids -> !isSorted(ids));

        assertThat(splits.outerSplit())
            .matches(outerSplits -> outerSplits.testSet().size() == numberOfExamples * testFraction)
            .matches(outerSplits -> outerSplits.trainSet().size() == numberOfExamples * (1 - testFraction));
    }

    @Test
    void testRandomnessIsIndependentOfIdMapping() {
        var originalIds1 = List.of(5L, 7L, 2L, 6L, 1L, 4L, 9L, 11L, 0L, 8L, 3L, 10L);
        var splits1 = makeSplits(originalIds1);
        var originalIds2 = List.of(5L, 6L, 7L, 2L, 4L, 0L, 1L, 9L, 11L, 8L, 3L, 10L);
        var splits2 = makeSplits(originalIds2);

        var result1 = new ArrayList<Long>();
        var result2 = new ArrayList<Long>();
        for (long id : splits1.outerSplit().trainSet().toArray()) {
            result1.add(originalIds1.get((int) id));
        }
        for (long id : splits2.outerSplit().trainSet().toArray()) {
            result2.add(originalIds2.get((int) id));
        }
        assertThat(result1).isEqualTo(result2);
    }

    @Test
    void shouldLogWarnings() {
        var progressTracker = new InspectableTestProgressTracker(Tasks.leaf("DUMMY"), "", new JobId());
        int numberOfExamples = 12;
        var splitter = new NodeSplitter(
            new Concurrency(4),
            numberOfExamples,
            progressTracker,
            i -> i,
            i -> i
        );

        double testFraction = 0.25;
        int validationFolds = 3;
        splitter.split(testFraction, validationFolds, Optional.of(42L));

        assertThat(progressTracker.log().getMessages(TestLog.WARN))
            .extracting(removingThreadId())
            .containsExactly(
                "DUMMY :: The specified `testFraction` leads to a very small test set with only 3 node(s). " +
                "Proceeding with such a small set might lead to unreliable results.",
                "DUMMY :: The specified `validationFolds` leads to very small validation sets with only 3 node(s). " +
                "Proceeding with such small sets might lead to unreliable results."
            );

        assertThat(progressTracker.log().getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "DUMMY :: Train set size is 9",
                "DUMMY :: Test set size is 3"
            );
    }

    @NotNull
    private NodeSplitter.NodeSplits makeSplits(List<Long> originalIds) {
        int numberOfExamples = originalIds.size();
        var toMappedIds = new long[numberOfExamples];
        for (int i = 0; i < originalIds.size(); i++) {
            toMappedIds[Math.toIntExact(originalIds.get(i))] = i;
        }
        var splitter = new NodeSplitter(
            new Concurrency(4),
            numberOfExamples,
            ProgressTracker.NULL_TRACKER,
            i -> originalIds.get((int) i),
            i -> toMappedIds[(int) i]
        );

        double testFraction = 0.25;
        int validationFolds = 3;
        return splitter.split(testFraction, validationFolds, Optional.of(42L));
    }

    boolean isSorted(long... elements) {
        long lastElement = -1;
        for (long element : elements) {
            if (element < lastElement) {
                return false;
            }
            lastElement = element;
        }

        return true;
    }

}
