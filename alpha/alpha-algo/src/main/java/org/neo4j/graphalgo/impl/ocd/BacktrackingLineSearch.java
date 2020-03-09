/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.ocd;

public class BacktrackingLineSearch {
    private static final double C = 0.5;
    private static final double TAU = 0.8;
    private static final double LR = 100;
    private static final int MAX_ITERATIONS = 1000;

    public double search(GainFunction gain, SparseVector point, SparseVector gradient) {
        double gradientL2 = gradient.l2();
        double lossAtPoint = gain.gain();
        double lr = LR;
        int iterations = 0;
        while (simulateStep(gain, point, gradient, gradientL2, lossAtPoint, lr) < C && iterations < MAX_ITERATIONS) {
            lr *= TAU;
            iterations++;
        }
        if (iterations == MAX_ITERATIONS) {
            System.out.println("Stopped Backtracking line search after max iterations.");
            return 0;
        }
        return lr;
    }

    private double simulateStep(GainFunction gain, SparseVector point, SparseVector gradient, double gradientL2, double lossAtPoint, double lr) {
        SparseVector candidate = point.addAndProject(gradient.multiply(lr));
        SparseVector diff = candidate.subtract(point);
        double expectedGain = diff.innerProduct(gradient);
        double increase = gain.gain(diff) - lossAtPoint;
        return increase / expectedGain;
    }
}
