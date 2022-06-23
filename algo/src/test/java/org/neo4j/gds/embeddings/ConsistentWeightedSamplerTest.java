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
package org.neo4j.gds.embeddings;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ConsistentWeightedSamplerTest {

    @Test
    void shouldSampleLinearSearch() {
        var segmentCenters = HugeDoubleArray.of(0.41, 0.3, 0.1, 0.01, 0.4, 0.8);
        var probabilities = HugeDoubleArray.of(0.2, 0.1, 0.6, 0.1);
        var nodeIds = HugeLongArray.of(3, 2, 0, 5);
        var sampler = new ConsistentWeightedSampler(segmentCenters, probabilities, nodeIds);

        var hits = new int[(int) segmentCenters.size()];
        var nodeIdsSet = Arrays.stream(nodeIds.toArray()).boxed().collect(Collectors.toSet());
        for (int i = 0; i < 1000; i++) {
            long seed = 42 + i;
            long sample = sampler.sample(seed);
            assertThat(nodeIdsSet.contains(sample)).isTrue();
            hits[(int) sample]++;
        }
        for (int i = 0; i < nodeIds.size(); i++) {
            assertThat(hits[(int) nodeIds.get(i)] / 1000.0).isCloseTo(probabilities.get(i), Offset.offset(0.05));
        }
    }

    @Test
    void shouldSampleBinarySearch() {
        var segmentCenters = HugeDoubleArray.of(0.41, 0.3, 0.1, 0.01, 0.4, 0.8, 0.34, 0.92, 0.14, 0.25);
        var probabilities = HugeDoubleArray.of(0.15, 0.05, 0.05, 0.1, 0.2, 0.1, 0.2, 0.1, 0.15);
        var nodeIds = HugeLongArray.of(3, 2, 0, 5, 9, 1, 4, 6, 7);
        var sampler = new ConsistentWeightedSampler(segmentCenters, probabilities, nodeIds);

        var hits = new int[(int) segmentCenters.size()];
        var nodeIdsSet = Arrays.stream(nodeIds.toArray()).boxed().collect(Collectors.toSet());
        for (int i = 0; i < 1000; i++) {
            long seed = 42 + i;
            long sample = sampler.sample(seed);
            assertThat(nodeIdsSet.contains(sample)).isTrue();
            hits[(int) sample]++;
        }
        for (int i = 0; i < nodeIds.size(); i++) {
            assertThat(hits[(int) nodeIds.get(i)] / 1000.0).isCloseTo(probabilities.get(i), Offset.offset(0.05));
        }
    }

    private HugeDoubleArray probabilitiesVector(long size, Random random) {
        var probabilities = HugeDoubleArray.newArray(size);

        probabilities.setAll(i -> random.nextDouble());

        double sum = probabilities.stream().sum();
        probabilities.setAll(i -> probabilities.get(i) / sum);

        return probabilities;
    }

    @Test
    void shouldSampleConsistently() {
        long size = 20;
        var random = new Random(42);

        var segmentCenters = HugeDoubleArray.newArray(size);
        segmentCenters.setAll(i -> random.nextDouble());

        var probabilities1 = probabilitiesVector(size, random);
        var probabilities2 = probabilitiesVector(size, random);

        double minSum = 0;
        double maxSum = 0;
        for (int i = 0; i < probabilities1.size(); i++) {
            minSum += Math.min(probabilities1.get(i), probabilities2.get(i));
            maxSum += Math.max(probabilities1.get(i), probabilities2.get(i));
        }
        double jaccard = minSum / maxSum;

        var nodeIds = HugeLongArray.newArray(size);
        nodeIds.setAll(i -> i);
        var sampler1 = new ConsistentWeightedSampler(segmentCenters, probabilities1, nodeIds);
        var sampler2 = new ConsistentWeightedSampler(segmentCenters, probabilities2, nodeIds);

        int collisions = 0;
        for (int i = 0; i < 1000; i++) {
            long seed = 42 + i;
            long sample1 = sampler1.sample(seed);
            long sample2 = sampler2.sample(seed);
            if (sample1 == sample2) {
                collisions++;
            }
        }
        assertThat(collisions / 1000.0).isCloseTo(jaccard, Offset.offset(0.03));
    }
}
