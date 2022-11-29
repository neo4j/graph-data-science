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
package org.neo4j.gds.ml.models.logisticregression;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.TrainingMethod;

import java.io.Serializable;

@ValueClass
public interface LogisticRegressionData extends Classifier.ClassifierData, Serializable {

    Weights<Matrix> weights();
    Weights<Vector> bias();

    @Value.Derived
    default TrainingMethod trainerMethod() {
        return TrainingMethod.LogisticRegression;
    }

    @Value.Derived
    default int featureDimension() {
        return weights().dimension(Dimensions.COLUMNS_INDEX);
    }

    static LogisticRegressionData standard(int featureCount, int numberOfClasses) {
        return create(numberOfClasses, featureCount, false);
    }

    // this is an optimization where "virtually" add a weight of 0.0 for the last class
    static LogisticRegressionData withReducedClassCount(int featureCount, int numberOfClasses) {
        return create(numberOfClasses, featureCount, true);
    }

    private static LogisticRegressionData create(int classCount, int featureCount, boolean skipLastClass) {
        var rows = skipLastClass ? classCount - 1 : classCount;

        var weights = Weights.ofMatrix(rows, featureCount);

        var bias = new Weights<>(new Vector(rows));

        return ImmutableLogisticRegressionData
            .builder()
            .weights(weights)
            .numberOfClasses(classCount)
            .bias(bias)
            .build();
    }

    static MemoryEstimation memoryEstimation(boolean isReduced, int numberOfClasses, MemoryRange featureDimension) {
        int normalizedNumberOfClasses = isReduced ? (numberOfClasses - 1) : numberOfClasses;
        return MemoryEstimations.builder("Logistic regression model data")
            .fixed("weights", featureDimension.apply(featureDim -> Weights.sizeInBytes(
                normalizedNumberOfClasses,
                Math.toIntExact(featureDim)
            )))
            .fixed("bias", Weights.sizeInBytes(normalizedNumberOfClasses, 1))
            .build();
    }

    static ImmutableLogisticRegressionData.Builder builder() {
        return ImmutableLogisticRegressionData.builder();
    }
}
