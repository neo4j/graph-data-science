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
package org.neo4j.gds.ml.core.decisiontree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class FeatureBaggerTest {

    private static final int TOTAL_INDICES = 20;
    private static final int[] BAG = new int[10];
    private static final FeatureBagger featureBagger = new FeatureBagger(new Random(), TOTAL_INDICES);

    @BeforeEach
    void setup() {
        Arrays.fill(BAG, 0);
    }

    @Test
    void shouldSampleValidInterval() {
        featureBagger.sample(BAG);

        for (int i : BAG) {
            assertThat(i).isGreaterThanOrEqualTo(0).isLessThan(TOTAL_INDICES);
        }
    }

    @Test
    void shouldSampleWithoutReplacement() {
        var sampledIndices = new BitSet(TOTAL_INDICES);

        featureBagger.sample(BAG);

        for (int i : BAG) {
            assertThat(sampledIndices.get(i)).isFalse();
            sampledIndices.set(i);
        }
    }
}
