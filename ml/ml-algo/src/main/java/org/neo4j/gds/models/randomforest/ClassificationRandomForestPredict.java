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
package org.neo4j.gds.models.randomforest;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.decisiontree.DecisionTreePredict;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.models.Classifier;
import org.neo4j.gds.models.Features;

public class ClassificationRandomForestPredict implements Classifier {

    private final DecisionTreePredict<Long>[] decisionTrees;
    private final LocalIdMap classMapping;

    public ClassificationRandomForestPredict(
        DecisionTreePredict<Long>[] decisionTrees,
        LocalIdMap classMapping
    ) {
        this.decisionTrees = decisionTrees;
        this.classMapping = classMapping;
    }

    @Override
    public LocalIdMap classIdMap() {
        return classMapping;
    }

    @Override
    public ClassifierData data() {
        return ImmutableRandomForestData.of(decisionTrees);
    }

    @Override
    public double[] predictProbabilities(long id, Features features) {
        return predictProbabilities(features.get(id));
    }

    @Override
    public Matrix predictProbabilities(
        Batch batch, Features features
    ) {
        var predictedProbabilities = new Matrix(batch.size(), numberOfClasses());
        var offset = 0;

        for (long id : batch.nodeIds()) {
            predictedProbabilities.setRow(offset++, predictProbabilities(id, features));
        }

        return predictedProbabilities;
    }

    public double[] predictProbabilities(double[] features) {
        int[] votesPerClass = gatherTreePredictions(features);

        double[] probabilities = new double[votesPerClass.length];

        for (int classIdx = 0; classIdx < votesPerClass.length; classIdx++) {
            int voteForClass = votesPerClass[classIdx];
            probabilities[classIdx] = (double) voteForClass / decisionTrees.length;
        }

        return probabilities;
    }

    @TestOnly
    long predictLabel(final double[] features) {
        final int[] predictionsPerClass = gatherTreePredictions(features);

        int max = -1;
        int maxClassIdx = 0;

        for (int i = 0; i < predictionsPerClass.length; i++) {
            var numPredictions = predictionsPerClass[i];

            if (numPredictions <= max) continue;

            max = numPredictions;
            maxClassIdx = i;
        }

        return classMapping.toOriginal(maxClassIdx);
    }

    private int[] gatherTreePredictions(double[] features) {
        final var predictionsPerClass = new int[classMapping.size()];

        for (DecisionTreePredict<Long> decisionTree : decisionTrees) {
            long predictedClass = decisionTree.predict(features);
            predictionsPerClass[classMapping.toMapped(predictedClass)]++;
        }
        return predictionsPerClass;
    }
}
