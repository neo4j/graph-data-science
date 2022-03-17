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
package org.neo4j.gds.ml.decisiontree;

import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

class FeatureBaggerTest {

    private static final int TOTAL_INDICES = 20;
    private static final FeatureBagger FEATURE_BAGGER = new FeatureBagger(new SplittableRandom(), TOTAL_INDICES, 0.5);

    @Test
    void shouldSampleValidInterval() {
        var bag = FEATURE_BAGGER.sample();

        for (int i : bag) {
            assertThat(i).isBetween(0, TOTAL_INDICES - 1);
        }
    }

    @Test
    void shouldSampleWithoutReplacement() {
        var sampledIndices = new BitSet(TOTAL_INDICES);

        var bag = FEATURE_BAGGER.sample();

        for (int i : bag) {
            assertThat(sampledIndices.get(i)).isFalse();
            sampledIndices.set(i);
        }
    }

    @Test
    void shouldSampleCorrectlyForSuperHighRatio() {
        var sampledIndices = new BitSet(TOTAL_INDICES);
        var featureBagger = new FeatureBagger(new SplittableRandom(), TOTAL_INDICES, 0.99999);

        var bag = featureBagger.sample();

        for (int i : bag) {
            assertThat(sampledIndices.get(i)).isFalse();
            sampledIndices.set(i);
        }
    }
}
