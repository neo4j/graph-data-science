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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeByteArray;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class DatasetBootstrapperTest {

    private static final Random RANDOM = new Random();

    @Test
    void shouldSampleCorrectNumElements() {
        var cachedBootstrappedDataset = HugeByteArray.newArray(
            20,
            AllocationTracker.empty()
        );
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            0.5,
            cachedBootstrappedDataset,
            AllocationTracker.empty()
        );

        assertThat(bootstrappedVectors.size()).isEqualTo(10L);
    }

    @Test
    void shouldSampleConsistentlyWithCache() {
        var cachedBootstrappedDataset = HugeByteArray.newArray(
            20,
            AllocationTracker.empty()
        );
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            0.5,
            cachedBootstrappedDataset,
            AllocationTracker.empty()
        );

        for (long i = 0; i < cachedBootstrappedDataset.size(); i++) {
            byte sampleIdx = cachedBootstrappedDataset.get(i);

            boolean found = false;
            for (long j = 0; j < bootstrappedVectors.size(); j++) {
                if (bootstrappedVectors.get(j) == i) {
                    found = true;
                }
            }

            if (sampleIdx == (byte) 1) {
                assertThat(found).isTrue();
            } else {
                assertThat(found).isFalse();
            }
        }
    }

    @Test
    void shouldSampleCorrectInterval() {
        var cachedBootstrappedDataset = HugeByteArray.newArray(
            20,
            AllocationTracker.empty()
        );
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            0.5,
            cachedBootstrappedDataset,
            AllocationTracker.empty()
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
        var cachedBootstrappedDataset = HugeByteArray.newArray(
            4,
            AllocationTracker.empty()
        );
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            random,
            1.0,
            cachedBootstrappedDataset,
            AllocationTracker.empty()
        );

        assertThat(bootstrappedVectors.get(0)).isEqualTo(0);
        assertThat(bootstrappedVectors.get(1)).isEqualTo(0);
        assertThat(bootstrappedVectors.get(2)).isEqualTo(3);
        assertThat(bootstrappedVectors.get(3)).isEqualTo(3);
    }
}
