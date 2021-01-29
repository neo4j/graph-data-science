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

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.ml.linkmodels.SignedProbabilities;

public enum LinkMetric {
    AUCPR;

    public double compute(
        SignedProbabilities signedProbabilities,
        double classRatio
    ) {
        if (signedProbabilities.positiveCount() == 0) return 0.0;
        var auc = new MutableDouble(0);
        var lastPrecision = new MutableDouble(
            signedProbabilities.positiveCount()
            / (signedProbabilities.positiveCount() + classRatio * signedProbabilities.negativeCount())
        );
        var lastRecall = new MutableDouble(1.0);
        MutableLong positivesSeen = new MutableLong(0);
        MutableLong negativesSeen = new MutableLong(0);
        signedProbabilities.stream().forEach(signedProbability -> {
            boolean isPositive = signedProbability > 0;
            if (isPositive) {
                positivesSeen.increment();
            } else {
                negativesSeen.increment();
            }
            var truePositives = signedProbabilities.positiveCount() - positivesSeen.getValue();
            if (truePositives == 0) {
                auc.add(lastPrecision.getValue() * (lastRecall.getValue()));
                lastPrecision.setValue(0);
                lastRecall.setValue(0);
                return;
            }
            var falsePositives = signedProbabilities.negativeCount() - negativesSeen.getValue();
            var falseNegatives = positivesSeen.getValue();
            //TODO: consider if BigDecimal division is needed for large graphs
            var precision = truePositives/(truePositives + classRatio * falsePositives);
            var recall = truePositives/((double)(truePositives + falseNegatives));
            auc.add(lastPrecision.getValue() * (lastRecall.getValue() - recall));
            lastPrecision.setValue(precision);
            lastRecall.setValue(recall);
        });
        return auc.getValue();
    }
}
