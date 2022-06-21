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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.TrainingMethod;

import java.util.Random;

@ValueClass
public interface MLPClassifierData extends Classifier.ClassifierData {

    //1 hidden layer with fixed size
    Weights<Matrix> inputWeights();

    Weights<Matrix> outputWeights();

    Weights<Vector> inputBias();

    Weights<Vector> outputBias();

    default TrainingMethod trainerMethod() {return TrainingMethod.MLPClassification;}

    static MLPClassifierData create(int classCount, int featureCount) {

        return ImmutableMLPClassifierData
            .builder()
            .inputWeights(generateWeights(featureCount,featureCount,42L))
            .outputWeights(generateWeights(classCount,featureCount,42L))
            .inputBias(new Weights<>(Vector.create(0.1,featureCount)))
            .outputBias(new Weights<>(Vector.create(0.1,classCount)))
            .numberOfClasses(classCount)
            .featureDimension(featureCount)
            .build();
    }

    //TODO: Refactor to use LayerFactory and ActivationFunction in algo.embeddings.graphsage
    private static Weights<Matrix> generateWeights(int rows, int cols, long randomSeed) {
        var weightBound = Math.sqrt(2d / cols);
        double[] data = new Random(randomSeed)
            .doubles(Math.multiplyExact(rows, cols), -weightBound, weightBound)
            .toArray();

        return new Weights<>(new Matrix(
            data,
            rows,
            cols
        ));
    }
}
