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
package org.neo4j.gds.models.randomforest;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class DatasetBootstrapperTest {

    private static final Random RANDOM = new Random();

    @Test
    void shouldSampleCorrectNumElements() {
        int numVectors = 20;
        var cachedBootstrappedDataset = new BitSet(numVectors);
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            0.5,
            numVectors,
            cachedBootstrappedDataset
        );

        assertThat(bootstrappedVectors.size()).isEqualTo(10L);
    }

    @Test
    void shouldSampleConsistentlyWithCache() {
        int numVectors = 20;
        var cachedBootstrappedDataset = new BitSet(numVectors);
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            0.5,
            numVectors,
            cachedBootstrappedDataset
        );

        for (long i = 0; i < cachedBootstrappedDataset.size(); i++) {
            boolean found = false;
            for (long j = 0; j < bootstrappedVectors.size(); j++) {
                if (bootstrappedVectors.get(j) == i) {
                    found = true;
                }
            }

            if (cachedBootstrappedDataset.get(i)) {
                assertThat(found).isTrue();
            } else {
                assertThat(found).isFalse();
            }
        }
    }

    @Test
    void shouldSampleCorrectInterval() {
        int numVectors = 20;
        var cachedBootstrappedDataset = new BitSet(numVectors);
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            0.5,
            numVectors,
            cachedBootstrappedDataset
        );

        for (long i = 0; i < bootstrappedVectors.size(); i++) {
            assertThat(bootstrappedVectors.get(i))
                .isGreaterThanOrEqualTo(0)
                .isLessThan(cachedBootstrappedDataset.size());
        }
    }

    @Test
    void shouldSampleWithReplacement() {
        var random = new Random(1337);
        int numVectors = 4;
        var cachedBootstrappedDataset = new BitSet(numVectors);
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            random,
            1.0,
            numVectors,
            cachedBootstrappedDataset
        );

        assertThat(bootstrappedVectors.get(0)).isEqualTo(0);
        assertThat(bootstrappedVectors.get(1)).isEqualTo(0);
        assertThat(bootstrappedVectors.get(2)).isEqualTo(3);
        assertThat(bootstrappedVectors.get(3)).isEqualTo(3);
    }
}
