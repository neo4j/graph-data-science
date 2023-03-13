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

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.ml.api.TrainingMethod;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.models.Regressor;

@ValueClass
public interface LinearRegressionData extends Regressor.RegressorData {
    Weights<Matrix> weights();

    Weights<Scalar> bias();

    @Value.Derived
    default TrainingMethod trainerMethod() {
        return TrainingMethod.LinearRegression;
    }

    @Value.Derived
    default int featureDimension() {
        return weights().data().cols();
    }

    static LinearRegressionData of(int featureDimension) {
        return ImmutableLinearRegressionData.builder()
            .weights(Weights.ofMatrix(1, featureDimension))
            .bias(Weights.ofScalar(0D))
            .build();
    }
}
