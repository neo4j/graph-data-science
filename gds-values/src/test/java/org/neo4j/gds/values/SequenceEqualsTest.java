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
package org.neo4j.gds.values;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SequenceEqualsTest {

    @Test
    void differingLengthsAreNeverEqual() {
        assertThat(SequenceEquals.byteAndInt(new byte[]{1, 2}, new int[]{1, 2, 3})).isFalse();
        assertThat(SequenceEquals.longAndDouble(new long[]{1}, new double[]{})).isFalse();
        assertThat(SequenceEquals.floatAndDouble(new float[]{}, new double[]{0D})).isFalse();
    }

    @Test
    void emptySequencesAreEqual() {
        assertThat(SequenceEquals.byteAndLong(new byte[]{}, new long[]{})).isTrue();
        assertThat(SequenceEquals.floatAndDouble(new float[]{}, new double[]{})).isTrue();
    }

    @Test
    void integralComparisonsAreExact() {
        assertThat(SequenceEquals.byteAndShort(new byte[]{-1, 2}, new short[]{-1, 2})).isTrue();
        assertThat(SequenceEquals.byteAndShort(new byte[]{-1}, new short[]{255})).isFalse();
        assertThat(SequenceEquals.intAndLong(
            new int[]{Integer.MIN_VALUE},
            new long[]{Integer.MIN_VALUE}
        )).isTrue();
        assertThat(SequenceEquals.intAndLong(new int[]{1}, new long[]{1L << 40})).isFalse();
    }

    @Test
    void comparingAgainstFloatsNarrowsTheOtherSideToFloat() {
        // 2^24 + 1 is the first long that float cannot represent; it rounds down to 2^24
        assertThat(SequenceEquals.longAndFloat(new long[]{16_777_217L}, new float[]{16_777_216F})).isTrue();
        assertThat(SequenceEquals.intAndFloat(new int[]{16_777_217}, new float[]{16_777_216F})).isTrue();
        // one below the precision limit still compares exactly
        assertThat(SequenceEquals.longAndFloat(new long[]{16_777_215L}, new float[]{16_777_216F})).isFalse();
    }

    @Test
    void comparingAgainstDoublesNarrowsTheOtherSideToDouble() {
        // 2^53 + 1 is the first long that double cannot represent
        var beyondDoublePrecision = (1L << 53) + 1;
        assertThat(SequenceEquals.longAndDouble(
            new long[]{beyondDoublePrecision},
            new double[]{(double) (1L << 53)}
        )).isTrue();
        assertThat(SequenceEquals.longAndDouble(new long[]{1L << 53}, new double[]{(double) (1L << 53)})).isTrue();
    }

    @Test
    void aFloatWidensToDoubleWithoutLoss() {
        assertThat(SequenceEquals.floatAndDouble(new float[]{0.5F}, new double[]{0.5D})).isTrue();
        // 0.1F is not 0.1D, so the two are not equal
        assertThat(SequenceEquals.floatAndDouble(new float[]{0.1F}, new double[]{0.1D})).isFalse();
        assertThat(SequenceEquals.floatAndDouble(new float[]{0.1F}, new double[]{0.1F})).isTrue();
    }

    @Test
    void nanIsNeverEqualToNan() {
        assertThat(SequenceEquals.floatAndDouble(
            new float[]{Float.NaN},
            new double[]{Double.NaN}
        )).isFalse();
        assertThat(SequenceEquals.longAndDouble(new long[]{0L}, new double[]{Double.NaN})).isFalse();
    }

    @Test
    void negativeZeroEqualsZero() {
        assertThat(SequenceEquals.floatAndDouble(new float[]{-0.0F}, new double[]{0.0D})).isTrue();
        assertThat(SequenceEquals.longAndDouble(new long[]{0L}, new double[]{-0.0D})).isTrue();
    }

    @Test
    void infinitiesCompareByValue() {
        assertThat(SequenceEquals.floatAndDouble(
            new float[]{Float.POSITIVE_INFINITY},
            new double[]{Double.POSITIVE_INFINITY}
        )).isTrue();
        assertThat(SequenceEquals.floatAndDouble(
            new float[]{Float.POSITIVE_INFINITY},
            new double[]{Double.NEGATIVE_INFINITY}
        )).isFalse();
    }
}