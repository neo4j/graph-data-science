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
package org.neo4j.gds.embeddings.node2vec;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.concurrency.Concurrency;

import static org.assertj.core.api.Assertions.assertThat;

 class RandomWalkProbabilitiesTest {

    @Test
    void shouldProduceSamplesAccordingToNodeDistribution() {
        double positiveSamplingFactor = 0.001;
        double negativeSamplingExponent = 0.75;
        var builder = new RandomWalkProbabilities.Builder(
            2,
            new Concurrency(4),
            positiveSamplingFactor,
            negativeSamplingExponent
        );

        builder
            .registerWalk(new long[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

        builder.registerWalk(new long[]{1});

        RandomWalkProbabilities probabilityComputer = builder.build();

        var negSampling = probabilityComputer.negativeSamplingDistribution();
        var posSampling = probabilityComputer.positiveSamplingProbabilities();

        double app0 = 16;
        double app1 = 1;
        double sum = 17;
        double freq0 = app0/sum;
        double freq1 = app1/sum;

        var expectedPos0 =   (Math.sqrt(freq0/positiveSamplingFactor) + 1) * (positiveSamplingFactor/freq0);
        var expectedPos1 =   (Math.sqrt(freq1/positiveSamplingFactor) + 1) * (positiveSamplingFactor/freq1);

        assertThat(posSampling.get(0)).isCloseTo(expectedPos0, Offset.offset(1e-6));
        assertThat(posSampling.get(1)).isCloseTo(expectedPos1, Offset.offset(1e-6));

        //neg[i] = 2*pow(16,negativeSamplingExponent) + neg[i-1]
        long expectedNeg0 = 2 * (long) Math.pow(app0, negativeSamplingExponent);
        long expectedNeg1 = 2 * (long) Math.pow(app1, negativeSamplingExponent) + expectedNeg0 ;

        assertThat(negSampling.get(0)).isEqualTo(expectedNeg0);
        assertThat(negSampling.get(1)).isEqualTo(expectedNeg1);

    }
}
