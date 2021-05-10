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

import org.neo4j.gds.ml.core.AbstractVariable;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;

public class CrossEntropyLoss extends AbstractVariable<Scalar> {
    private final Variable<Matrix> predictions;
    private final Variable<Vector> targets;

    public CrossEntropyLoss(Variable<Matrix> predictions, Variable<Vector> targets) {
        super(List.of(predictions, targets), Dimensions.scalar());
        this.predictions = predictions;
        this.targets = targets;
    }

    public static long sizeInBytes() {
        return sizeOfDoubleArray(1);
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        var predictionsMatrix = ctx.data(predictions);
        var targetsVector = ctx.data(targets);

        double result = 0;
        for (int row = 0; row < targetsVector.totalSize(); row++) {
            var trueClass = (int) targetsVector.dataAt(row);
            var predictedProbabilityForTrueClass =
                predictionsMatrix.dataAt(row * predictionsMatrix.cols() + trueClass);
            if (predictedProbabilityForTrueClass > 0) {
                result += Math.log(predictedProbabilityForTrueClass);
            }
        }

        return new Scalar(-result / predictionsMatrix.rows());
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        if (parent == predictions) {
            var predictionsMatrix = ctx.data(predictions);
            Matrix gradient = predictionsMatrix.zeros();
            var targetsVector = ctx.data(targets);

            var multiplier = -1.0 / gradient.rows();
            for (int row = 0; row < gradient.rows(); row++) {
                var trueClass = (int) targetsVector.dataAt(row);
                var predictedProbabilityForTrueClass = predictionsMatrix.dataAt(row * predictionsMatrix.cols() + trueClass);
                if (predictedProbabilityForTrueClass > 0) {
                    gradient.setDataAt(
                        row * predictionsMatrix.cols() + trueClass,
                        multiplier / predictedProbabilityForTrueClass
                    );
                }
            }
            return gradient;
        } else {
            return ctx.data(parent).zeros();
        }
    }
}
