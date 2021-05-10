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
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.api.ImmutableRelationshipCursor;

import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class WeightedUniformReservoirRSamplerTest {

    @Test
    void shouldSampleSubsetOfNeighbors() {
        var highWeightNodes = Set.of(33L, 66L, 99L);
        int numberOfSamples = highWeightNodes.size();

        var sampled = new LongLongHashMap();
        var sampler = new WeightedUniformReservoirRSampler(19L);

        var input = LongStream.range(1, 100).mapToObj(targetId -> {
            var weight = highWeightNodes.contains(targetId) ? 99D : 1D;
            return ImmutableRelationshipCursor.of(0, targetId, weight);
        }).collect(Collectors.toList());

        var tries = 1000;
        for (int i = 0; i < tries; i++) {
            sampler.sample(input.stream(), input.size(), numberOfSamples)
                .forEach(nodeId -> sampled.addTo(nodeId, 1));
        }

        assertThat(sampled.keys()).hasSize(input.size());

        highWeightNodes
            .forEach(highNode -> {
                var highSample = sampled.remove(highNode);
                assertThat(highSample)
                    .withFailMessage("Unexpected sample %d for high weight node %d", highSample, highNode)
                    .isCloseTo(680L, Offset.offset(20L));
        });

        sampled.forEach((Consumer<LongLongCursor>) lowNodeSamples ->
            assertThat(lowNodeSamples.value)
                .withFailMessage("Unexpected sample %d for low weight node %d", lowNodeSamples.value, lowNodeSamples.key)
                .isCloseTo(10L, Offset.offset(10L))
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 4, 17, 99})
    void shouldSampleTheCorrectNumber(int numberOfSamples) {
        var input = LongStream.range(0, 18).mapToObj(targetId -> {
            var weight = 1D;
            return ImmutableRelationshipCursor.of(0, targetId, weight);
        }).collect(Collectors.toList());

        var sampler = new WeightedUniformReservoirRSampler(19L);
        var sample = sampler.sample(input.stream(), 18, numberOfSamples);

        var expectedSize = Math.min(18, numberOfSamples);

        assertThat(sample)
            .hasSize(expectedSize)
            .doesNotHaveDuplicates();
    }

    @Test
    void duplicateElements() {
        var input = LongStream.of(1, 1, 1).mapToObj(targetId -> {
            var weight = 1D;
            return ImmutableRelationshipCursor.of(0, targetId, weight);
        }).collect(Collectors.toList());


        var sampler = new WeightedUniformReservoirRSampler(19L);
        var sample = sampler.sample(input.stream(), 3,2).toArray();

        assertThat(sample).containsExactly(1, 1);
    }

    @Test
    void zeroWeights() {
        var input = LongStream.of(1, 1, 1)
            .mapToObj(targetId -> ImmutableRelationshipCursor.of(0, targetId, 0D)).collect(Collectors.toList());

        var sampler = new WeightedUniformReservoirRSampler(19L);
        var samples = sampler.sample(input.stream(), 3, 2).toArray();

        assertThat(samples).hasSize(2);
    }
}
