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

import org.neo4j.gds.ml.linkmodels.SignedProbabilities;

public enum LinkMetric {
    AUCPR;

    public double compute(SignedProbabilities signedProbabilities, double classRatio) {
        var positiveCount = signedProbabilities.positiveCount();
        var negativeCount = signedProbabilities.negativeCount();
        if (positiveCount == 0) return 0.0;
        var curveConsumer = new CurveConsumer();
        var signedProbabilitiesConsumer = new SignedProbabilitiesConsumer(
            curveConsumer,
            positiveCount,
            negativeCount,
            classRatio
        );
        curveConsumer.acceptFirstPoint(
            signedProbabilitiesConsumer.recall(positiveCount),
            signedProbabilitiesConsumer.precision(positiveCount)
        );
        signedProbabilities.stream().forEach(signedProbabilitiesConsumer::accept);
        curveConsumer.accept(0.0, 1.0);

        return curveConsumer.auc();
    }

    private static class SignedProbabilitiesConsumer {
        private final CurveConsumer innerConsumer;
        private final long positiveCount;
        private final long negativeCount;
        private final double classRatio;

        private double lastThreshold;

        private long positivesSeen;
        private long negativesSeen;

        private SignedProbabilitiesConsumer(
            CurveConsumer innerConsumer,
            long positiveCount,
            long negativeCount,
            double classRatio
        ) {
            this.innerConsumer = innerConsumer;
            this.positiveCount = positiveCount;
            this.negativeCount = negativeCount;
            this.classRatio = classRatio;
            this.positivesSeen = 0;
            this.negativesSeen = 0;
        }

        void accept(double signedProbability) {
            var hasSeenAValue = positivesSeen > 0 || negativesSeen > 0;
            if (hasSeenAValue) {
                if (Math.abs(signedProbability) != lastThreshold) {
                    reportPointOnCurve();
                }
            }
            lastThreshold = Math.abs(signedProbability);
            if (signedProbability >= 0) {
                positivesSeen++;
            } else {
                negativesSeen++;
            }

        }

        private void reportPointOnCurve() {
            var truePositives = positiveCount - positivesSeen;
            if (truePositives == 0) {
                innerConsumer.accept(0.0, 0.0);
            } else {
                innerConsumer.accept(recall(truePositives), precision(truePositives));
            }
        }

        private double precision(double truePositives) {
            var falsePositives = negativeCount - negativesSeen;
            return truePositives / (truePositives + classRatio * falsePositives);
        }

        private double recall(double truePositives) {
            var falseNegatives = positivesSeen;
            return truePositives / (truePositives + falseNegatives);

        }
    }

    private static class CurveConsumer {

        private double auc;
        private double previousYcoordinate;
        private double previousXcoordinate;

        void acceptFirstPoint(double x, double y) {
            this.previousXcoordinate = x;
            this.previousYcoordinate = y;
        }

        void accept(double x, double y) {
            auc += (previousYcoordinate + y) / 2.0 * (previousXcoordinate - x);
            this.previousXcoordinate = x;
            this.previousYcoordinate = y;
        }

        double auc() {
            return auc;
        }

    }
}
