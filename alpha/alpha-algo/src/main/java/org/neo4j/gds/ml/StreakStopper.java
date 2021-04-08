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

class StreakStopper implements TrainingStopper {

    private final int minIterations;
    private final int patience;
    private final int maxIterations;
    private final int windowSize;
    private final double tolerance;

    private int count;
    private double bestMovingAverage;
    private int unproductiveStreak;
    private final double[] lossHistory;

    StreakStopper(int minIterations, int patience, int maxIterations, int windowSize, double tolerance) {
        this.minIterations = minIterations;
        this.patience = patience;
        this.maxIterations = maxIterations;
        this.windowSize = windowSize;
        this.tolerance = tolerance;
        this.lossHistory = new double[windowSize];
        this.bestMovingAverage = Double.MAX_VALUE;
    }

    private double movingAverage() {
        double sum = 0;
        for (double loss : lossHistory) {
            sum += loss;
        }
        return sum / Math.min(count, windowSize);
    }

    @Override
    public void registerLoss(double loss) {
        if(terminated()) {
            return; // or throw???
        }
        if(count >= minIterations) {
            if (loss - bestMovingAverage >= - tolerance * Math.abs(bestMovingAverage)) {
                unproductiveStreak++;
            } else {
                unproductiveStreak = 0;
            }
        }

        lossHistory[count % windowSize] = loss;
        count++;
        bestMovingAverage = Math.min(bestMovingAverage, movingAverage());
    }

    @Override
    public boolean terminated() {
        return count >= maxIterations ||
               unproductiveStreak >= patience;
    }

    @Override
    public boolean converged() {
        return unproductiveStreak >= patience;
    }
}
