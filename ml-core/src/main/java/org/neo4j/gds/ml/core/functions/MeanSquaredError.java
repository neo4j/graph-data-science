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

import java.util.List;

public class MeanSquaredError extends AbstractVariable<Scalar> {
    private final Variable<Matrix> predictions;
    private final Variable<Matrix> targets;

    public MeanSquaredError(
        Variable<Matrix> predictions,
        Variable<Matrix> targets
    ) {
        super(List.of(predictions, targets), Dimensions.scalar());
        this.predictions = predictions;
        this.targets = targets;
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        Tensor<?> predictedData = ctx.data(predictions);
        Tensor<?> targetData = ctx.data(targets);
        double sumOfSquares = 0;
        for (int i = 0; i < predictedData.totalSize(); i++) {
            sumOfSquares += Math.pow((predictedData.dataAt(i) - targetData.dataAt(i)), 2);
        }

        return new Scalar(sumOfSquares/predictedData.totalSize());
    }

    @Override
    public Tensor<?> gradient(
        Variable<?> parent, ComputationContext ctx
    ) {
        // targets should be a constant, so gradient should not really be required
        Tensor<?> parentData = ctx.data(parent);
        Tensor<?> notParentData = parent == predictions ? ctx.data(targets) : ctx.data(predictions);
        double[] grad = new double[parentData.totalSize()];
        double scale = ctx.gradient(this).data()[0]/parentData.totalSize();
        for (int i = 0; i < parentData.totalSize(); i++) {
            grad[i] = scale * 2 * (parentData.dataAt(i) - notParentData.dataAt(i));
        }

        return new Matrix(grad, parentData.totalSize(), 1);
    }
}
