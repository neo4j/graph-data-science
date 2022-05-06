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
package org.neo4j.gds.ml.models.linearregression;

import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.EWiseAddMatrixScalar;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.models.Regressor;

public class LinearRegressor implements Regressor {
    private final LinearRegressionData data;

    public LinearRegressor(LinearRegressionData data) {this.data = data;}

    @Override
    public double predict(double[] features) {
        Matrix weights = this.data.weights().data();

        double prediction = 0;

        for (int i = 0; i < data.featureDimension(); i++) {
            prediction += weights.dataAt(i) * features[i];
        }

        return prediction + this.data.bias().data().value();
    }

    Variable<Matrix> predictionsVariable(Variable<Matrix> features) {
        var weightedFeatures = new MatrixMultiplyWithTransposedSecondOperand(
            features,
            data.weights()
        );

        return new EWiseAddMatrixScalar(weightedFeatures, data.bias());
    }

    @Override
    public LinearRegressionData data() {
        return data;
    }
}
