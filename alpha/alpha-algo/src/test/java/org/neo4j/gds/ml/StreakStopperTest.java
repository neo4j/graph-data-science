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
package org.neo4j.gds.ml;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StreakStopperTest {

    private StreakStopper stopper;
    private final static int MIN_ITERATION = 10;
    private final static int MAX_STREAK = 3;
    private final static int MAX_REGISTRATIONS = 1000;
    private final static int WINDOW_SIZE = 5;
    private final static double TOLERANCE = 1e-4;

    @BeforeEach
    void setup() {
        stopper = new StreakStopper(MIN_ITERATION, MAX_STREAK, MAX_REGISTRATIONS, WINDOW_SIZE, TOLERANCE);
    }

    @Test
    void shouldMakeAtLeastTenIterations() {
        for (int i = 0; i < MIN_ITERATION; i++) {
            stopper.registerLoss(0.11);
        }

        assertThat(stopper.terminated()).isFalse();
    }

    @Test
    void shouldTerminateAfterMaxStreakUnproductiveHits() {
        for (int i = 0; i < MIN_ITERATION; i++) {
            stopper.registerLoss(0.11);
        }

        assertThat(stopper.terminated()).isFalse();

        for (int i = 0; i < MAX_STREAK; i++) {
            stopper.registerLoss(0.11);
        }

        assertThat(stopper.terminated()).isTrue();
    }

    @Test
    void shouldTerminateAfterMaxRegistrations() {
        var startLoss = 0.13;

        int i = 0;
        while (i < 2*MAX_REGISTRATIONS && !stopper.terminated()) {
            stopper.registerLoss(startLoss);
            startLoss -= 0.1;
            i++;
        }

        assertThat(stopper.terminated()).isTrue();
        assertThat(i).isEqualTo(MAX_REGISTRATIONS);
    }

    @Test
    void shouldTerminateEarlyConstantNegativeLoss() {
        var startLoss = -0.13;

        int i = 0;
        while (i < 2*MAX_REGISTRATIONS && !stopper.terminated()) {
            stopper.registerLoss(startLoss);
            i++;
        }

        assertThat(stopper.terminated()).isTrue();
        assertThat(i).isEqualTo(MIN_ITERATION + MAX_STREAK);
    }

    @Test
    void shouldTerminateIfImprovingButWorseThanEarlier() {
        var startLoss = 1.0;

        int i = 0;
        while (i < MIN_ITERATION && !stopper.terminated()) {
            stopper.registerLoss(startLoss);
            i++;
        }
        stopper.registerLoss(-2);
        stopper.registerLoss(0.7);
        stopper.registerLoss(0.6);
        assertThat(stopper.terminated()).isFalse();
        stopper.registerLoss(0.5);
        assertThat(stopper.terminated()).isTrue();
    }
}
