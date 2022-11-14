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

import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.CrossEntropyLoss;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.FocalLoss;
import org.neo4j.gds.ml.core.functions.L2NormSquared;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.gradientdescent.Objective;
import org.neo4j.gds.ml.models.Features;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MLPClassifierObjective implements Objective<MLPClassifierData> {

    private final MLPClassifier classifier;
    private final Features features;
    private final HugeIntArray labels;

    private final double penalty;

    private final double focusWeight;

    private final double[] classWeights;


    public MLPClassifierObjective(MLPClassifier classifier, Features features, HugeIntArray labels, double penalty, double focusWeight,
                                  double[] classWeights
    ) {
        this.classifier = classifier;
        this.features = features;
        this.labels = labels;
        this.penalty = penalty;
        this.focusWeight = focusWeight;
        this.classWeights = classWeights;
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        List<Weights<? extends Tensor<?>>> combinedWeights = new ArrayList<>();
        combinedWeights.addAll(classifier.data().weights());
        combinedWeights.addAll(classifier.data().biases());
        return combinedWeights;
    }

    @Override
    public Variable<Scalar> loss(Batch batch, long trainSize) {
        var crossEntropyLoss = crossEntropyLoss(batch);
        var penalty = penaltyForBatch(batch, trainSize);
        return new ElementSum(List.of(crossEntropyLoss, penalty));
    }

    CrossEntropyLoss crossEntropyLoss(Batch batch) {
        var batchLabels = batchLabelVector(batch);
        var batchFeatures = Objective.batchFeatureMatrix(batch, features);
        var predictions = classifier.predictionsVariable(batchFeatures);
        if (focusWeight == 0) {
            return new CrossEntropyLoss(
                predictions,
                batchLabels,
                classWeights
            );
        } else {
            return new FocalLoss(predictions, batchLabels, focusWeight, classWeights);
        }
    }

    ConstantScale<Scalar> penaltyForBatch(Batch batch, long trainSize) {
        List<Variable<?>> L2Norms = classifier.data().weights().stream().map(L2NormSquared::new).collect(
            Collectors.toList());
        return new ConstantScale<>(new ElementSum(L2Norms), batch.size() * penalty / trainSize);
    }

    Constant<Vector> batchLabelVector(Batch batch) {
        var batchedTargets = new Vector(batch.size());
        var batchOffset = 0;
        var batchIterator = batch.elementIds();

        while (batchIterator.hasNext()) {
            var elementId = batchIterator.nextLong();
            batchedTargets.setDataAt(batchOffset++, labels.get(elementId));
        }

        return new Constant<>(batchedTargets);
    }

    @Override
    public MLPClassifierData modelData() {
        return classifier.data();
    }
}
