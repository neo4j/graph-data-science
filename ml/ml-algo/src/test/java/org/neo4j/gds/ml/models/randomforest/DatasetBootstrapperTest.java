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
package org.neo4j.gds.ml.models.randomforest;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import java.util.HashSet;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class DatasetBootstrapperTest {

    private static final SplittableRandom RANDOM = new SplittableRandom();

    @Test
    void shouldSampleCorrectNumElements() {
        long numVectors = 20;
        HugeLongArray mutableTrainSet = HugeLongArray.newArray(numVectors);
        mutableTrainSet.setAll(idx -> idx);
        var trainSet = ReadOnlyHugeLongArray.of(mutableTrainSet);

        var cachedBootstrappedDataset = new BitSet(numVectors);
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            0.5,
            trainSet,
            cachedBootstrappedDataset
        );

        assertThat(bootstrappedVectors.size()).isEqualTo(10L);
    }

    @Test
    void shouldSampleConsistentlyWithCache() {
        long numVectors = 20;
        HugeLongArray mutableTrainSet = HugeLongArray.newArray(numVectors);
        mutableTrainSet.setAll(idx -> idx);
        var trainSet = ReadOnlyHugeLongArray.of(mutableTrainSet);

        var cachedBootstrappedDataset = new BitSet(numVectors);
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            0.5,
            trainSet,
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
        long numVectors = 20;
        HugeLongArray mutableTrainSet = HugeLongArray.newArray(numVectors);
        mutableTrainSet.setAll(idx -> idx);
        var trainSet = ReadOnlyHugeLongArray.of(mutableTrainSet);

        var cachedBootstrappedDataset = new BitSet(numVectors);
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            0.5,
            trainSet,
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
        var random = new SplittableRandom(1337);

        long numVectors = 4;
        HugeLongArray mutableTrainSet = HugeLongArray.newArray(numVectors);
        mutableTrainSet.setAll(idx -> idx);
        var trainSet = ReadOnlyHugeLongArray.of(mutableTrainSet);

        var cachedBootstrappedDataset = new BitSet(numVectors);
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            random,
            1.0,
            trainSet,
            cachedBootstrappedDataset
        );

        assertThat(bootstrappedVectors.get(0)).isEqualTo(3);
        assertThat(bootstrappedVectors.get(1)).isEqualTo(1);
        assertThat(bootstrappedVectors.get(2)).isEqualTo(2);
        assertThat(bootstrappedVectors.get(3)).isEqualTo(2);
    }

    @Test
    void shouldOnlySampleFromTrainsSet() {
        long numberOfAllVectors = 40;
        double trainFraction = 0.1;
        HugeLongArray mutableTrainSet = HugeLongArray.newArray((long) (numberOfAllVectors / trainFraction));
        mutableTrainSet.setAll(idx -> 5 * idx);
        var trainSetArray = mutableTrainSet.toArray();
        var trainSet = ReadOnlyHugeLongArray.of(mutableTrainSet);

        var cachedBootstrappedDataset = new BitSet(trainSet.size());
        var bootstrappedVectors = DatasetBootstrapper.bootstrap(
            RANDOM,
            1.0,
            trainSet,
            cachedBootstrappedDataset
        );

        var distinctBootstrappedVectors = new HashSet<>();
        for (long i = 0; i < bootstrappedVectors.size(); i++) {
            long sampledAllVectorsIdx = bootstrappedVectors.get(i);
            assertThat(trainSetArray).contains(sampledAllVectorsIdx);

            var trainSetIdx = IntStream.range(0, trainSetArray.length)
                .filter(idx -> trainSetArray[idx] == sampledAllVectorsIdx)
                .findFirst()
                .orElseThrow();

            assertThat(cachedBootstrappedDataset.get(trainSetIdx)).isTrue();
            distinctBootstrappedVectors.add(trainSetIdx);
        }

        // exactly one bit set for each distinct sampled vector
        assertThat(cachedBootstrappedDataset.cardinality()).isEqualTo(distinctBootstrappedVectors.size());
    }

}
