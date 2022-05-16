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
package org.neo4j.gds.ml.gradientdescent;

class StreakStopper implements TrainingStopper {

    private final int minEpochs;
    private final int patience;
    private final int maxEpochs;
    private final double tolerance;

    private int ranEpochs;
    private double bestLoss;
    private int unproductiveStreak;

    StreakStopper(int minEpochs, int patience, int maxEpochs, double tolerance) {
        this.minEpochs = minEpochs;
        this.patience = patience;
        this.maxEpochs = maxEpochs;
        this.tolerance = tolerance;
        this.bestLoss = Double.MAX_VALUE;
    }

    @Override
    public void registerLoss(double loss) {
        if(terminated()) {
            throw new IllegalStateException("Does not accept losses after convergence");
        }
        if(ranEpochs >= minEpochs) {
            if (loss - bestLoss >= - tolerance * Math.abs(bestLoss)) {
                unproductiveStreak++;
            } else {
                unproductiveStreak = 0;
            }
        }

        ranEpochs++;
        bestLoss = Math.min(bestLoss, loss);
    }

    @Override
    public boolean terminated() {
        return ranEpochs >= maxEpochs ||
               unproductiveStreak >= patience;
    }

    @Override
    public boolean converged() {
        return unproductiveStreak >= patience;
    }
}
