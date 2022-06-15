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

@ValueClass
public interface MLPClassifierData extends Classifier.ClassifierData {

    //1 hidden layer with fixed size
    Weights<Matrix> inputWeights();
    Weights<Matrix> outputWeights();
    Weights<Vector> inputBias();
    Weights<Vector> outputBias();

    default TrainingMethod trainerMethod() { return TrainingMethod.MLPClassification; }

    //TODO Check indexing same with common libraries
    static MLPClassifierData create(int classCount, int featureCount) {
        var inputWeights = Weights.ofMatrix(featureCount, featureCount);
        var outputWeights = Weights.ofMatrix(classCount, featureCount);
        var inputBias =  new Weights<>(new Vector(featureCount));
        var outputBias = new Weights<>(new Vector(classCount));


        return ImmutableMLPClassifierData
            .builder()
            .inputWeights(inputWeights)
            .outputWeights(outputWeights)
            .inputBias(inputBias)
            .outputBias(outputBias)
            .numberOfClasses(classCount)
            .featureDimension(featureCount)
            .build();
    }




}
