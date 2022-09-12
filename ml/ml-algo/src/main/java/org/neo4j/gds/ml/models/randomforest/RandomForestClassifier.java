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
package org.neo4j.gds.ml.models.randomforest;

import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredictor;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.Features;

import java.util.List;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfIntArray;

public class RandomForestClassifier implements Classifier {

    private final RandomForestClassifierData data;

    public RandomForestClassifier(
        List<DecisionTreePredictor<Integer>> decisionTrees,
        int numberOfClasses,
        int featureDimension
    ) {
        this(ImmutableRandomForestClassifierData.of(featureDimension, numberOfClasses, decisionTrees));
    }

    public RandomForestClassifier(RandomForestClassifierData data) {
        this.data = data;
    }

    public static MemoryRange runtimeOverheadMemoryEstimation(int numberOfClasses) {
        return MemoryRange.of(sizeOfInstance(RandomForestClassifier.class))
            .add(sizeOfDoubleArray(numberOfClasses))
            .add(sizeOfIntArray(numberOfClasses));
    }

    @Override
    public ClassifierData data() {
        return data;
    }

    @Override
    public double[] predictProbabilities(double[] features) {
        int[] votesPerClass = gatherTreePredictions(features);
        int numberOfTrees = data.decisionTrees().size();

        double[] probabilities = new double[numberOfClasses()];

        for (int classIdx = 0; classIdx < votesPerClass.length; classIdx++) {
            int voteForClass = votesPerClass[classIdx];
            probabilities[classIdx] = (double) voteForClass / numberOfTrees;
        }

        return probabilities;
    }

    @Override
    public Matrix predictProbabilities(
        Batch batch, Features features
    ) {
        var predictedProbabilities = new Matrix(batch.size(), numberOfClasses());
        var offset = 0;
        var batchIterator = batch.elementIds();

        while (batchIterator.hasNext()) {
            var id = batchIterator.nextLong();
            predictedProbabilities.setRow(offset++, predictProbabilities(features.get(id)));
        }

        return predictedProbabilities;
    }

    int[] gatherTreePredictions(double[] features) {
        final var predictionsPerClass = new int[numberOfClasses()];

        for (DecisionTreePredictor<Integer> decisionTree : data.decisionTrees()) {
            int predictedClass = decisionTree.predict(features);
            predictionsPerClass[predictedClass]++;
        }
        return predictionsPerClass;
    }
}
