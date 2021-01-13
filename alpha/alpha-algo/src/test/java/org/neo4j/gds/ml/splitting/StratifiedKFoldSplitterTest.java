/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import org.bouncycastle.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class StratifiedKFoldSplitterTest {

    @Test
    void shouldGive2CorrectSplits() {
        var nodeIds = HugeLongArray.of(2, 0, 1);
        var targets = HugeLongArray.of(42, 42, 5);
        var kFoldSplitter = new StratifiedKFoldSplitter(2, nodeIds, targets);
        var splits = kFoldSplitter.splits();
        assertThat(splits.size()).isEqualTo(2);
        assertThat(splits.get(0).testSet().size()).isEqualTo(2);
        assertThat(splits.get(0).trainSet().size()).isEqualTo(1);
        assertThat(splits.get(1).testSet().size()).isEqualTo(1);
        assertThat(splits.get(1).trainSet().size()).isEqualTo(2);

        // Because of stratification both test and train sets should both contain a nodeId that corresponds to target 42
        assertThat(splits.get(0).testSet().toArray()).satisfiesAnyOf(this::containsZero, this::containsOne);
        assertThat(splits.get(0).trainSet().toArray()).satisfiesAnyOf(this::containsZero, this::containsOne);
        assertThat(splits.get(1).testSet().toArray()).satisfiesAnyOf(this::containsZero, this::containsOne);
        assertThat(splits.get(1).trainSet().toArray()).satisfiesAnyOf(this::containsZero, this::containsOne);
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L})
    void shouldGive5CorrectSplits(long seed) {

        var nodeIdsList = new ArrayList<Long>() {{
            addAll(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L));
        }};

        Collections.shuffle(nodeIdsList, new Random(seed));

        var nodeIds = HugeLongArray.newArray(nodeIdsList.size(), AllocationTracker.empty());
        nodeIds.setAll(i -> nodeIdsList.get((int) i));
        var targets = HugeLongArray.of(0, 1, 1, 1, 0, 0, 0, 0, 1, 1);
        var kFoldSplitter = new StratifiedKFoldSplitter(5, nodeIds, targets);
        var splits = kFoldSplitter.splits();
        assertThat(splits.size()).isEqualTo(5);
        for (int fold = 0; fold < 5; fold++) {
            assertThat(splits.get(fold).testSet().size()).isEqualTo(2);
            assertThat(splits.get(fold).trainSet().size()).isEqualTo(8);
            var firstTestNode = splits.get(fold).testSet().get(0);
            var secondTestNode = splits.get(fold).testSet().get(1);
            // test stratification on test set. if stratified then train set is also stratified correctly.
            assertThat(targets.get(firstTestNode)).isNotEqualTo(targets.get(secondTestNode));
        }
    }

    private boolean containsZero(long[] values) {
        return Arrays.contains(values, 0L);
    }

    private boolean containsOne(long[] values) {
        return Arrays.contains(values, 1L);
    }


}
