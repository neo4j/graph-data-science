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
package org.neo4j.gds.ml.metrics.classification;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class F1ScoreTest {

    private HugeLongArray originalTargets;
    private HugeLongArray originalPredictions;

    private HugeIntArray targets;
    private HugeIntArray predictions;

    private LocalIdMap localIdMap;

    @BeforeEach
    void setup() {
        originalPredictions = HugeLongArray.of(3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4);
        originalTargets = HugeLongArray.of(4, 4, 5, 5, 5, 8, 9, 1, 1, 2, 2, 3, 3, 4, 5);
        localIdMap = LocalIdMap.of(Arrays.stream(originalTargets.toArray()).toArray());
        predictions = HugeIntArray.newArray(originalPredictions.size());
        predictions.setAll(index -> localIdMap.toMapped(originalPredictions.get(index)));
        targets = HugeIntArray.newArray(originalTargets.size());
        targets.setAll(index -> localIdMap.toMapped(originalTargets.get(index)));
    }

    @Test
    void shouldComputeF1BinaryBadModel() {
        var binaryPredictions = HugeIntArray.of(0, 0, 0, 0, 0);
        var binaryTargets = HugeIntArray.of(0, 0, 0, 0, 1);
        var metric = new F1Score(1, localIdMap.toMapped(1));
        assertThat(metric.compute(binaryTargets, binaryPredictions)).isEqualTo(0.0);
    }

    @Test
    void shouldComputeF1BinaryMediumModel() {
        var binaryPredictions = HugeIntArray.of(0, 0, 0, 0, 1);
        var binaryTargets = HugeIntArray.of(0, 0, 0, 1, 1);
        var metric = new F1Score(1, 1);
        var precision = 1.0;
        var recall = 0.5;
        var f1 = 2.0/(1.0/precision + 1.0/recall);
        assertThat(metric.compute(binaryTargets, binaryPredictions)).isCloseTo(f1, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1BinaryPerfectModel() {
        var binaryPredictions = HugeIntArray.of(0, 0, 0, 1, 1);
        var binaryTargets = HugeIntArray.of(0, 0, 0, 1, 1);
        var metric = new F1Score(1, 1);
        assertThat(metric.compute(binaryTargets, binaryPredictions)).isCloseTo(1.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1AllCorrectMultiple() {
        var metric = new F1Score(1, localIdMap.toMapped(1));
        assertThat(metric.compute(targets, predictions)).isCloseTo(1.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1MissedSome() {
        var metric = new F1Score(2, localIdMap.toMapped(2));
        assertThat(metric.compute(targets, predictions)).isCloseTo(2.0 / 3.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1PredictedSomeExtra() {
        var metric = new F1Score(3, localIdMap.toMapped(3));
        assertThat(metric.compute(targets, predictions)).isCloseTo(2.0 / 3.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1MissedSomePredictedSomeExtra() {
        var metric = new F1Score(4, localIdMap.toMapped(4));
        assertThat(metric.compute(targets, predictions)).isCloseTo(2.0 / 3.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1MissedAll() {
        var metric = new F1Score(5, localIdMap.toMapped(5));
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1NoSuchTarget() {
        var metric = new F1Score(6, localIdMap.toMapped(6));
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1NoSuchTargetNoSuchPrediction() {
        var metric = new F1Score(99, localIdMap.toMapped(99));
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1NoSuchTargetSinglePrediction() {
        var metric = new F1Score(7, localIdMap.toMapped(7));
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeF1MissedAllAndPredictedAllWrong() {
        var metric = new F1Score(8, localIdMap.toMapped(8));
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
        var metric2 = new F1Score(9, localIdMap.toMapped(9));
        assertThat(metric2.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }
}
