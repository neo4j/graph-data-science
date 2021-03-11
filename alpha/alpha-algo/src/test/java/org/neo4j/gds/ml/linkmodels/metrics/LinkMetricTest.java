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


import org.apache.commons.io.IOUtils;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.linkmodels.SignedProbabilities;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class LinkMetricTest {

    /**
     * Example computation lifted from https://sanchom.wordpress.com/tag/average-precision/
     * However updating to use trapezoid rule.
     */
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
        var r10 = 1.0;
        var p10 = 0.5;
        var r9 = 0.8;
        var p9 = 4.0/9.0;
        var r8 = 0.8;
        var p8 = 4.0/8.0;
        var r7 = 0.8;
        var p7 = 4.0/7.0;
        var r6 = 0.8;
        var p6 = 4.0/6.0;
        var r5 = 0.6;
        var p5 = 3.0/5.0;
        var r4 = 0.6;
        var p4 = 3.0/4.0;
        var r3 = 0.4;
        var p3 = 2.0/3.0;
        var r2 = 0.4;
        var p2 = 2.0/2.0;
        var r1 = 0.2;
        var p1 = 1.0/1.0;
        var r0 = 0.0;
        var p0 = 1.0;

        var area10to9 = (r10 - r9) * (p10 + p9) / 2.0;
        var area9to8 = (r9 - r8) * (p9 + p8) / 2.0;
        var area8to7 = (r8 - r7) * (p8 + p7) / 2.0;
        var area7to6 = (r7 - r6) * (p7 + p6) / 2.0;
        var area6to5 = (r6 - r5) * (p6 + p5) / 2.0;
        var area5to4 = (r5 - r4) * (p5 + p4) / 2.0;
        var area4to3 = (r4 - r3) * (p4 + p3) / 2.0;
        var area3to2 = (r3 - r2) * (p3 + p2) / 2.0;
        var area2to1 = (r2 - r1) * (p2 + p1) / 2.0;
        var area1to0 = (r1 - r0) * (p1 + p0) / 2.0;

        double expectedAUCScore = area10to9 + area9to8 + area8to7 + area7to6 + area6to5
                                  + area5to4 + area4to3 + area3to2 + area2to1 + area1to0;

        var aucScore = LinkMetric.AUCPR.compute(signedProbabilities, 1.0);
        assertThat(aucScore).isCloseTo(expectedAUCScore, Offset.offset(1e-24));
    }

    @Test
    void shouldComputeAUCPRWithClassratio() {
        var signedProbabilities = SignedProbabilities.create(4);
        signedProbabilities.add(4);
        signedProbabilities.add(-3);
        signedProbabilities.add(-2);
        signedProbabilities.add(1);
        // r4 means recall when extracting 4 examples , r3 means recall when extracting 3 examples etc
        var r4 = 1.0;
        var p4 = 2.0/22.0;
        var r3 = 0.5;
        var p3 = 1.0/21.0;
        var r2 = 0.5;
        var p2 = 1.0/11.0;
        var r1 = 0.5;
        var p1 = 1.0;
        var r0 = 0.0;
        var p0 = 1.0;

        var area4to3 = (r4 - r3) * (p4 + p3) / 2.0;
        var area3to2 = (r3 - r2) * (p3 + p2) / 2.0;
        var area2to1 = (r2 - r1) * (p2 + p1) / 2.0;
        var area1to0 = (r1 - r0) * (p1 + p0) / 2.0;

        double expectedAUCScore = area4to3 + area3to2 + area2to1 + area1to0;
        var aucScore = LinkMetric.AUCPR.compute(signedProbabilities, 10.0);
        assertThat(aucScore).isCloseTo(expectedAUCScore, Offset.offset(1e-24));
    }

    @Test
    void shouldComputeAUCPRRepeatedScores() {
        var signedProbabilities = SignedProbabilities.create(7);
        signedProbabilities.add(-4);
        signedProbabilities.add(4);
        signedProbabilities.add(-4);
        signedProbabilities.add(3);
        signedProbabilities.add(2);
        signedProbabilities.add(-2);
        signedProbabilities.add(1);
        var aucScore = LinkMetric.AUCPR.compute(signedProbabilities, 1.0);
        // r4 means recall when extracting 4 groups , r3 means recall when extracting 3 groups etc
        var r4 = 1.0;
        var p4 = 4.0/7.0;
        var r3 = 0.75;
        var p3 = 3.0/6.0;
        var r2 = 0.5;
        var p2 = 0.5;
        var r1 = 0.25;
        var p1 = 1.0/3.0;
        var r0 = 0.0;
        var p0 = 1.0;

        var area4to3 = (r4 - r3) * (p4 + p3) / 2.0;
        var area3to2 = (r3 - r2) * (p3 + p2) / 2.0;
        var area2to1 = (r2 - r1) * (p2 + p1) / 2.0;
        var area1to0 = (r1 - r0) * (p1 + p0) / 2.0;

        double expectedAUCScore = area4to3 + area3to2 + area2to1 + area1to0;
        assertThat(aucScore).isCloseTo(expectedAUCScore, Offset.offset(1e-24));
    }

    @Test
    void shouldComputeAUCPRHighestPriorityElementIsNegative() {
        var signedProbabilities = SignedProbabilities.create(4);
        signedProbabilities.add(-4);
        signedProbabilities.add(3);
        signedProbabilities.add(-2);
        signedProbabilities.add(1);
        var aucScore = LinkMetric.AUCPR.compute(signedProbabilities, 1.0);
        // r4 means recall when extracting 4 examples , r3 means recall when extracting 3 examples etc
        var r4 = 1.0;
        var p4 = 0.5;
        var r3 = 0.5;
        var p3 = 1.0/3.0;
        var r2 = 0.5;
        var p2 = 0.5;
        var r1 = 0.0;
        var p1 = 0.0;

        var area4to3 = (r4 - r3) * (p4 + p3) / 2.0;
        var area3to2 = (r3 - r2) * (p3 + p2) / 2.0;
        var area2to1 = (r2 - r1) * (p2 + p1) / 2.0;

        double expectedAUCScore = area4to3 + area3to2 + area2to1;
        assertThat(aucScore).isCloseTo(expectedAUCScore, Offset.offset(1e-24));
    }

    @Test
    void shouldComputeSklearnAUC() throws IOException {
        var expectedAUC = 0.5077889503776734;
        var resourceAsStream = getClass().getResourceAsStream("/predictions.csv");
        var signedProbabilites = SignedProbabilities.create(1000);
        IOUtils.readLines(resourceAsStream, StandardCharsets.UTF_8).forEach(line -> {
            var split = line.split(",");
            var prob = Float.valueOf(split[1]);
            signedProbabilites.add(split[0].equals("+") ? prob : -prob);
        });
        assertThat(LinkMetric.AUCPR.compute(signedProbabilites, 1.0)).isEqualTo(expectedAUC, Offset.offset(1e-24));
    }
}
