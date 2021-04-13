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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

class FractionSplitterTest {
    @Test
    void shouldGiveEmptySets() {
        var fractionSplitter = new FractionSplitter(AllocationTracker.empty());
        var split = fractionSplitter.split(HugeLongArray.newArray(0, AllocationTracker.empty()), 0.5);
        Assertions.assertThat(split.trainSet().toArray()).isEmpty();
        Assertions.assertThat(split.testSet().toArray()).isEmpty();
    }

    @Test
    void shouldGiveCorrectFractionConsecutiveIds() {
        double fraction = 0.65;
        var fractionSplitter = new FractionSplitter(AllocationTracker.empty());
        var split = fractionSplitter.split(HugeLongArray.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), fraction);
        Assertions.assertThat(split.trainSet().toArray()).containsExactly(0, 1, 2, 3, 4, 5);
        Assertions.assertThat(split.testSet().toArray()).containsExactly(6, 7, 8, 9);
    }

    @Test
    void shouldGiveCorrectIdsForGeneralInput() {
        HugeLongArray input = HugeLongArray.of(4, 2, 1, 3, 3, 7);
        var fractionSplitter = new FractionSplitter(AllocationTracker.empty());
        var split = fractionSplitter.split(input, 0.6);

        Assertions.assertThat(split.trainSet().toArray()).containsExactly(4, 2, 1);
        Assertions.assertThat(split.testSet().toArray()).containsExactly(3, 3, 7);
    }


}
