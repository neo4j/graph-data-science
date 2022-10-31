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
package org.neo4j.gds.ml.core.functions;

import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;

public class FocalLoss extends CrossEntropyLoss {

    private final double focusWeight;

    public FocalLoss(
        Variable<Matrix> predictions,
        Variable<Vector> targets,
        double focusWeight
    ) {
        super(predictions, targets);
        this.focusWeight = focusWeight;
    }

    @Override
    double computeIndividualLoss(double predictedProbabilityForTrueClass) {
        double focalFactor = Math.pow(1 - predictedProbabilityForTrueClass, focusWeight);
        return focalFactor * Math.log(predictedProbabilityForTrueClass);
    }

    @Override
    double computeErrorPerExample(
        int numberOfExamples,
        double predictedProbabilityForTrueClass
    ) {
        var predictedProbabilityForWrongClasses = 1.0 - predictedProbabilityForTrueClass;
        var chainRuleGradient = Math.pow(predictedProbabilityForWrongClasses, focusWeight - 1.0);

        var focalLossPerExample = (focusWeight * chainRuleGradient * Math.log(predictedProbabilityForTrueClass)
                                   - chainRuleGradient * predictedProbabilityForWrongClasses / predictedProbabilityForTrueClass) / numberOfExamples;
        return focalLossPerExample;
    }
}
