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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LinearRegressorTest {

    @Test
    void predict() {
        var features = FeaturesFactory.wrap(List.of(
            new double[] {1, 2, 3},
            new double[] {4, 5, 6}
        ));

        int featureDimension = 3;
        LinearRegressionData modelData = LinearRegressionData.of(featureDimension);

        modelData.weights().data().mapInPlace(ignore -> 2);
        modelData.bias().data().setDataAt(0, 2.5);

        var regressor = new LinearRegressor(modelData);

        assertThat(regressor.data()).isSameAs(modelData);

        assertThat(regressor.predict(features.get(0))).isEqualTo(12 + 2.5);
        assertThat(regressor.predict(features.get(1))).isEqualTo(30 + 2.5);
    }

    @Test
    void predictionsVariable() {
        int featureDimension = 3;
        LinearRegressionData modelData = LinearRegressionData.of(featureDimension);

        modelData.weights().data().mapInPlace(ignore -> 2);
        modelData.bias().data().setDataAt(0, 2.5);

        var regressor = new LinearRegressor(modelData);

        Variable<Matrix> predictionsVariable = regressor.predictionsVariable(Constant.matrix(
            new double[]{1, 2, 3, 4, 5, 6},
            2,
            featureDimension
        ));

        Matrix predictions = new ComputationContext().forward(predictionsVariable);

        assertThat(predictions.dataAt(0)).isEqualTo(12 + 2.5);
        assertThat(predictions.dataAt(1)).isEqualTo(30 + 2.5);
    }
}
