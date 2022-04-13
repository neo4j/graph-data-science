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
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class KFoldSplitterTest {

    @Test
    void splitIntoKFolds() {
        KFoldSplitter splitter = new KFoldSplitter(
            3,
            ReadOnlyHugeLongArray.of(HugeLongArray.of(1, 2, 3, 4, 5, 6))
        );

        List<TrainingExamplesSplit> splits = splitter.splits();
        Stream<long[]> trainSets = splits.stream().map(split -> split.trainSet().toArray());
        Stream<long[]> testSets = splits.stream().map(split -> split.testSet().toArray());

        assertThat(trainSets).containsExactly(
            new long[]{3, 4, 5, 6},
            new long[]{5, 6, 1, 2},
            new long[]{1, 2, 3, 4}
        );

        assertThat(testSets).containsExactly(
             new long[]{1, 2},
             new long[]{3, 4},
             new long[]{5, 6}
        );
    }

    @Test
    void splitIntoKFoldsUneven() {
        KFoldSplitter splitter = new KFoldSplitter(
            3,
            ReadOnlyHugeLongArray.of(HugeLongArray.of(1, 2, 3, 4, 5, 6, 7, 8))
        );

        List<TrainingExamplesSplit> splits = splitter.splits();
        var trainSets = splits.stream().map(split -> split.trainSet().toArray());
        var testSets = splits.stream().map(split -> split.testSet().toArray());

        assertThat(trainSets).containsExactly(
            new long[]{3, 4, 5, 6, 7, 8},
            new long[]{5, 6, 7, 8, 1, 2},
            new long[]{7, 8, 1, 2, 3, 4}
        );

        assertThat(testSets).containsExactly(
            new long[]{1, 2},
            new long[]{3, 4},
            new long[]{5, 6}
        );
    }


    @ParameterizedTest
    @CsvSource(value = {
        " 1_000,  4, 0.5,  130904",
        " 1_000, 16, 0.5,  523544",
        " 1_000,  4, 0.2,   54104",
        "10_000,  4, 0.5, 1282904",
    })
    void memoryEstimation(long nodeCount, int k, double trainFraction, long expectedMemory) {
        MemoryRange actualEstimation = KFoldSplitter
            .memoryEstimationForNodeSet(k, trainFraction)
            .estimate(GraphDimensions.of(nodeCount), 4)
            .memoryUsage();

        MemoryRange expectedEstimation = MemoryRange.of(expectedMemory);

        assertThat(actualEstimation)
            .withFailMessage("Got %d, but expected %d", actualEstimation.max, expectedEstimation.max)
            .isEqualTo(expectedEstimation);
    }

}
