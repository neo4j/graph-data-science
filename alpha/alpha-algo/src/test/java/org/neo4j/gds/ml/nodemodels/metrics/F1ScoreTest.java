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
package org.neo4j.gds.ml.nodemodels.metrics;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;

class F1ScoreTest {

    private HugeLongArray targets;
    private HugeLongArray predictions;

    @BeforeEach
    void setup() {
        predictions = HugeLongArray.of(3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4);
        targets = HugeLongArray.of(4, 4, 5, 5, 5, 8, 9, 1, 1, 2, 2, 3, 3, 4, 5);

    }

    @Test
    void shouldComputeF1BinaryBadModel() {
        var binaryPredictions = HugeLongArray.of(0, 0, 0, 0, 0);
        var binaryTargets = HugeLongArray.of(0, 0, 0, 0, 1);
        var metric = new F1Score(1);
        assertThat(metric.compute(binaryTargets, binaryPredictions)).isEqualTo(0.0);
    }

    @Test
    void shouldComputeF1BinaryMediumModel() {
        var binaryPredictions = HugeLongArray.of(0, 0, 0, 0, 1);
        var binaryTargets = HugeLongArray.of(0, 0, 0, 1, 1);
        var metric = new F1Score(1);
        var precision = 1.0;
        var recall = 0.5;
        var f1 = 2.0/(1.0/precision + 1.0/recall);
        assertThat(metric.compute(binaryTargets, binaryPredictions)).isCloseTo(f1, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1BinaryPerfectModel() {
        var binaryPredictions = HugeLongArray.of(0, 0, 0, 1, 1);
        var binaryTargets = HugeLongArray.of(0, 0, 0, 1, 1);
        var metric = new F1Score(1);
        assertThat(metric.compute(binaryTargets, binaryPredictions)).isCloseTo(1.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1AllCorrectMultiple() {
        var metric = new F1Score(1);
        assertThat(metric.compute(targets, predictions)).isCloseTo(1.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1MissedSome() {
        var metric = new F1Score(2);
        assertThat(metric.compute(targets, predictions)).isCloseTo(2.0/3.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1PredictedSomeExtra() {
        var metric = new F1Score(3);
        assertThat(metric.compute(targets, predictions)).isCloseTo(2.0/3.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1MissedSomePredictedSomeExtra() {
        var metric = new F1Score(4);
        assertThat(metric.compute(targets, predictions)).isCloseTo(2.0/3.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1MissedAll() {
        var metric = new F1Score(5);
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1NoSuchTarget() {
        var metric = new F1Score(6);
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1NoSuchTargetNoSuchPrediction() {
        var metric = new F1Score(99);
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1NoSuchTargetSinglePrediction() {
        var metric = new F1Score(7);
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1MissedAllAndPredictedAllWrong() {
        var metric = new F1Score(8);
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
        var metric2 = new F1Score(9);
        assertThat(metric2.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }
}
