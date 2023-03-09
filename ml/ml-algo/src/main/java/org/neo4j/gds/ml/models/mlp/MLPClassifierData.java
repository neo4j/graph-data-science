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

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml_api.TrainingMethod;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;

@ValueClass
@SuppressWarnings("immutables:subtype")
public interface MLPClassifierData extends Classifier.ClassifierData, Serializable {

    List<Weights<Matrix>> weights();

    List<Weights<Vector>> biases();

    @Value.Derived
    default int depth() {return biases().size() + 1;}

    @Value.Derived
    @Override
    default int numberOfClasses() {return biases().get(biases().size()-1).dimension(0);}

    @Value.Derived
    default int featureDimension() {
        return weights().get(0).dimension(Dimensions.COLUMNS_INDEX);
    }


    default TrainingMethod trainerMethod() {return TrainingMethod.MLPClassification;}

    static MLPClassifierData create(int classCount, int featureCount, List<Integer> hiddenLayerSizes, SplittableRandom random) {

        var weights = new ArrayList<Weights<Matrix>>();
        var biases = new ArrayList<Weights<Vector>>();
        var hiddenDepth = hiddenLayerSizes.size();
        weights.add(generateWeights(hiddenLayerSizes.get(0), featureCount, random.nextLong()));
        biases.add(generateBias(hiddenLayerSizes.get(0), random.nextLong()));
        for (int i = 0; i < hiddenDepth-1; i++) {
            weights.add(generateWeights(hiddenLayerSizes.get(i+1), hiddenLayerSizes.get(i), random.nextLong()));
            biases.add(generateBias(hiddenLayerSizes.get(i+1), random.nextLong()));
        }
        weights.add(generateWeights(classCount, hiddenLayerSizes.get(hiddenDepth-1), random.nextLong()));
        biases.add(generateBias(classCount, random.nextLong()));

        return ImmutableMLPClassifierData
            .builder()
            .weights(weights)
            .biases(biases)
            .build();
    }

    //TODO: Refactor to use LayerFactory and ActivationFunction in algo.embeddings.graphsage
    //bounds for weights and biases are from Kaiming initialization for Relu: https://arxiv.org/abs/1502.01852
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

    private static Weights<Vector> generateBias(int dim, long randomSeed) {
        var weightBound = Math.sqrt(2d / dim);
        double[] data = new Random(randomSeed)
            .doubles(dim, -weightBound, weightBound)
            .toArray();

        return new Weights<>(new Vector(data));
    }

    static ImmutableMLPClassifierData.Builder builder() {
        return ImmutableMLPClassifierData.builder();
    }


}
