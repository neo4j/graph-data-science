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
package org.neo4j.gds.procedures.pipelines;

import org.HdrHistogram.DoubleHistogram;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HdrBackedGdsHistogramTest {

    @Test
    void recordsZeroProbabilitiesAsZero() {
        var histogram = new HdrBackedGdsHistogram();

        histogram.onPredictedLink(0.5);
        histogram.onPredictedLink(0.0);
        histogram.onPredictedLink(1.0);
        histogram.onPredictedLink(0.0);

        var summary = histogram.finalise();

        assertThat(summary).doesNotContainKey("Error");
        assertThat(summary).containsKeys("min", "max", "mean", "p50");
        assertThat((double) summary.get("min")).isEqualTo(0.0);
        assertThat((double) summary.get("max")).isCloseTo(1.0, Offset.offset(1E-3));
    }

    @Test
    void stopsRecordingAndDegradesWhenRangeCannotBeExtended() {
        var invocations = new AtomicInteger();
        var delegate = new DoubleHistogram(5) {
            @Override
            public void recordValue(double value) {
                invocations.incrementAndGet();
                throw new ArrayIndexOutOfBoundsException(
                    "The value " + value + " is out of bounds for histogram, current covered range " +
                    "[0.0625, 1.0) cannot be extended any further.\n" +
                    "Caused by: java.lang.ArrayIndexOutOfBoundsException: Operation would overflow, would discard recorded value counts"
                );
            }
        };
        var histogram = new HdrBackedGdsHistogram(delegate);

        assertThatCode(() -> {
            histogram.onPredictedLink(0.5);
            histogram.onPredictedLink(0.5);
            histogram.onPredictedLink(0.5);
        }).doesNotThrowAnyException();

        assertThat(invocations).hasValue(1);
        assertThat(histogram.finalise()).containsOnlyKeys("Error");
    }

    @Test
    void rethrowsUnrelatedHistogramFailures() {
        var delegate = new DoubleHistogram(5) {
            @Override
            public void recordValue(double value) {
                throw new ArrayIndexOutOfBoundsException("some unrelated histogram failure");
            }
        };
        var histogram = new HdrBackedGdsHistogram(delegate);

        assertThatThrownBy(() -> histogram.onPredictedLink(0.5))
            .isInstanceOf(ArrayIndexOutOfBoundsException.class)
            .hasMessage("some unrelated histogram failure");
    }
}
