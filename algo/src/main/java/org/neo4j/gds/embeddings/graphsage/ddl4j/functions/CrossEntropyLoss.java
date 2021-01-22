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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions;

import org.neo4j.gds.embeddings.graphsage.ddl4j.AbstractVariable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;

import java.util.List;

import static java.lang.Math.log;

/**
    Cross-entropy loss, or log loss, measures the performance of a classification model whose output is a probability value between 0 and 1.
    Cross-entropy loss increases as the predicted probability diverges from the actual label.

    The formula used by this variable is:
    −(𝑦log(𝑝)+(1−𝑦)log(1−𝑝))
 */
public class CrossEntropyLoss extends AbstractVariable<Scalar> {
    private final Variable<Matrix> predictions;
    private final Variable<Matrix> targets;

    public CrossEntropyLoss(Variable<Matrix> predictions, Variable<Matrix> targets) {
        super(List.of(predictions, targets), Dimensions.scalar());
        this.predictions = predictions;
        this.targets = targets;
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        Matrix predictionsVector = ctx.data(predictions);
        Matrix targetsVector = ctx.data(targets);

        double result = 0;
        for(int row = 0; row < targetsVector.totalSize(); row++) {
            if (predictionsVector.dataAt(row) > 0) {
                result += targetsVector.dataAt(row) * log(predictionsVector.dataAt(row));
            }
            if (predictionsVector.dataAt(row) < 1.0) {
                result += (1.0 - targetsVector.dataAt(row)) * log(1.0 - predictionsVector.dataAt(row));
            }
        }

        return new Scalar(-result/targetsVector.totalSize());
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        if (parent == predictions) {
            Matrix predictionsVector = ctx.data(predictions);
            Matrix targetsVector = ctx.data(targets);
            Matrix gradient = predictionsVector.zeros();
            double multiplier = 1.0 / gradient.totalSize();
            for (int row = 0; row < gradient.totalSize(); row++) {
                var result = 0d;
                if (predictionsVector.dataAt(row) > 0.0) {
                    result -= targetsVector.dataAt(row) / predictionsVector.dataAt(row);
                }
                if (predictionsVector.dataAt(row) < 1.0) {
                    result += (1.0 - targetsVector.dataAt(row)) / (1.0 - predictionsVector.dataAt(row));
                }
                gradient.setDataAt(row, multiplier * result);
            }
            return gradient;
        } else {
            return ctx.data(parent).zeros();
        }
    }
}
