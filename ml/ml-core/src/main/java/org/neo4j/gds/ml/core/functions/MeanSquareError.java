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
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;

import java.util.List;

import static org.neo4j.gds.ml.core.Dimensions.scalar;
import static org.neo4j.gds.ml.core.Dimensions.totalSize;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MeanSquareError extends AbstractVariable<Scalar> {
    private final Variable<?> predictions;
    private final Variable<?> targets;

    public MeanSquareError(
        Variable<?> predictions,
        Variable<?> targets
    ) {
        super(List.of(predictions, targets), scalar());
        this.predictions = predictions;
        this.targets = targets;

        validateDimensions(predictions, targets);
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        var predictedData = ctx.data(predictions);
        var targetData = ctx.data(targets);

        double sumOfSquares = 0;
        var length = predictedData.totalSize();
        for (int i = 0; i < length; i++) {
            double error = predictedData.dataAt(i) - targetData.dataAt(i);
            sumOfSquares += error * error;
        }

        if (!Double.isFinite(sumOfSquares)) {
            return new Scalar(Double.MAX_VALUE);
        }

        return new Scalar(sumOfSquares / length);
    }

    @Override
    public Tensor<?> gradient(
        Variable<?> parent, ComputationContext ctx
    ) {
        // targets should be a constant, so gradient should not really be required
        var parentData = ctx.data(parent);
        var otherParentData = parent == predictions ? ctx.data(targets) : ctx.data(predictions);

        var length = parentData.totalSize();
        var parentGradient = parentData.createWithSameDimensions();
        double scale = 2 * ctx.gradient(this).dataAt(0) / length;
        for (int i = 0; i < length; i++) {
            double error = parentData.dataAt(i) - otherParentData.dataAt(i);
            parentGradient.setDataAt(i, scale * error);
        }

        return parentGradient;
    }

    private void validateDimensions(Variable<?> predictions, Variable<?> targets) {
        if (totalSize(predictions.dimensions()) != totalSize(targets.dimensions())) {
            throw new IllegalArgumentException(formatWithLocale(
                "Targets and predictions must be of equal size. Got predictions: %s, targets: %s",
                Dimensions.render(predictions.dimensions()),
                Dimensions.render(targets.dimensions())
            ));
        }
    }
}
