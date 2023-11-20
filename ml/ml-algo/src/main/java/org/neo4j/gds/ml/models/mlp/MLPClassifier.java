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
package org.neo4j.gds.ml.models.mlp;

import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.MatrixVectorSum;
import org.neo4j.gds.ml.core.functions.Relu;
import org.neo4j.gds.ml.core.functions.Softmax;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.gradientdescent.Objective;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.Features;

public final class MLPClassifier implements Classifier {

    private final MLPClassifierData data;

    public MLPClassifier(MLPClassifierData data) {this.data = data;}

    @Override
    public double[] predictProbabilities(double[] features) {
        ComputationContext ctx = new ComputationContext();
        Constant<Matrix> featuresVariable = Constant.matrix(features, 1, features.length);
        Variable<Matrix> predictionsVariable = predictionsVariable(featuresVariable);
        var steps = ctx.forward(predictionsVariable).data();
        return steps;
    }

    @Override
    public Matrix predictProbabilities(Batch batch, Features features) {
        return new ComputationContext().forward(predictionsVariable(Objective.batchFeatureMatrix(batch, features)));
    }

    @Override
    public MLPClassifierData data() {
        return data;
    }

    Variable<Matrix> predictionsVariable(Constant<Matrix> batchFeatures) {

        Variable<Matrix> outputFromPrevLayer;
        Variable<Matrix> inputToNextLayer = batchFeatures;
        for (int i = 0; i < data.depth()-1; i++) {
            outputFromPrevLayer = inputToNextLayer;
            inputToNextLayer = new Relu<>(
                new MatrixVectorSum(
                    MatrixMultiplyWithTransposedSecondOperand.of(outputFromPrevLayer, data.weights().get(i)),
                    data.biases().get(i)),
                0);
        }

        return new Softmax(inputToNextLayer);
    }
}
