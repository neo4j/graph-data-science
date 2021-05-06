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
package org.neo4j.gds.ml.core.batch;

import com.carrotsearch.hppc.LongLongHashMap;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class UniformReservoirSamplerTest {

    @RepeatedTest(1000)
    void shouldSampleSubsetOfNeighbors() {
        int numberOfSamples = 2;

        var sampled = new LongLongHashMap();

        var sampler = new UniformReservoirSampler(19L);
        for (int i = 0; i < 20; i++) {
            var input = LongStream.range(0, 18);
            sampler.sample(input, 18, numberOfSamples)
                .forEach(nodeId -> sampled.addTo(nodeId, 1));
        }

        assertThat(sampled)
            .isNotEmpty()
            .allSatisfy(entry -> assertThat(entry.value)
                .withFailMessage("Sampled node with id %d %d times", entry.key, entry.value)
                .isLessThan(10));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 4, 17, 99})
    void shouldSampleTheCorrectNumber(int numberOfSamples) {
        var input = LongStream.range(0, 18);

        var sampler = new UniformReservoirSampler(19L);
        var sample = sampler.sample(input, 18, numberOfSamples);

        var expectedSize = Math.min(18, numberOfSamples);

        assertThat(sample)
            .hasSize(expectedSize)
            .doesNotHaveDuplicates();
    }

    @Test
    void duplicateElements() {
        var input = LongStream.of(1, 1, 1);

        var sampler = new UniformReservoirSampler(19L);
        var sample = sampler.sample(input, 3,2).toArray();

        assertThat(sample).containsExactly(1, 1);
    }
}
