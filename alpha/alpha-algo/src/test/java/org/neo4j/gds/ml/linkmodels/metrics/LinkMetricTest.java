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
package org.neo4j.gds.ml.linkmodels.metrics;


import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.linkmodels.SignedProbabilities;

import static org.assertj.core.api.Assertions.assertThat;

class LinkMetricTest {

    @Test
    void shouldComputeAUCPR() {
        var signedProbabilities = SignedProbabilities.create(10);
        signedProbabilities.add(10);
        signedProbabilities.add(9);
        signedProbabilities.add(-8);
        signedProbabilities.add(7);
        signedProbabilities.add(-6);
        signedProbabilities.add(5);
        signedProbabilities.add(-4);
        signedProbabilities.add(-3);
        signedProbabilities.add(-2);
        signedProbabilities.add(1);
        var aucScore = LinkMetric.AUCPR.compute(signedProbabilities, 1.0);
        assertThat(aucScore).isCloseTo(0.78333333333333333, Offset.offset(1e-6));
    }

    @Test
    void shouldComputeAUCPRHighestPriorityElementIsNegative() {
        var signedProbabilities = SignedProbabilities.create(10);
        signedProbabilities.add(-11);
        signedProbabilities.add(10);
        signedProbabilities.add(9);
        signedProbabilities.add(-8);
        signedProbabilities.add(7);
        signedProbabilities.add(-6);
        signedProbabilities.add(5);
        signedProbabilities.add(-4);
        signedProbabilities.add(-3);
        signedProbabilities.add(-2);
        signedProbabilities.add(1);
        var aucScore = LinkMetric.AUCPR.compute(signedProbabilities, 1.0);
        assertThat(aucScore).isCloseTo(0.2 * 5/11 + 0.2 * 4/7 + 0.2 * 3/5 + 0.2 * 2/3 + 0.2 * 1/2, Offset.offset(1e-6));
    }
}
