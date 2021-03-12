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
        var firstPrecision = positiveCount / (positiveCount + classRatio * negativeCount);
        var curveConsumer = new CurveConsumer(firstPrecision, 1.0);
        var signedProbabilitiesConsumer = new SignedProbabilitiesConsumer(
            curveConsumer,
            positiveCount,
            negativeCount,
            classRatio
        );
        signedProbabilities.stream().forEach(signedProbabilitiesConsumer::accept);
        curveConsumer.accept(1.0, 0.0);

        return curveConsumer.auc();
    }

    private class SignedProbabilitiesConsumer {
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
            var falsePositives = negativeCount - negativesSeen;
            var falseNegatives = positivesSeen;
            if (truePositives == 0) {
                innerConsumer.accept(0, 0);
                return;
            }
            var precision = truePositives/(truePositives + classRatio * falsePositives);
            var recall = truePositives/((double)(truePositives + falseNegatives));
            innerConsumer.accept(precision, recall);
        }
    }

    private class CurveConsumer {

        private double auc;
        private double previousPrecision;
        private double previousRecall;

        CurveConsumer(double previousPrecision, double previousRecall) {
            this.previousPrecision = previousPrecision;
            this.previousRecall = previousRecall;
        }

        void accept(double precision, double recall) {
            auc += (previousPrecision + precision)/2.0 * (previousRecall - recall);
            this.previousPrecision = precision;
            this.previousRecall = recall;
        }

        double auc() {
            return auc;
        }

    }
}
