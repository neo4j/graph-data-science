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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.CrossEntropyLoss;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.gradientdescent.Objective;
import org.neo4j.gds.ml.models.Features;

import java.util.List;

public class MLPClassifierObjective implements Objective<MLPClassifierData> {

    private final MLPClassifier classifier;
    private final Features features;
    private final HugeIntArray labels;

    public MLPClassifierObjective(MLPClassifier classifier, Features features, HugeIntArray labels) {
        this.classifier = classifier;
        this.features = features;
        this.labels = labels;
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(classifier.data().inputWeights(), classifier.data().outputWeights(), classifier.data()
            .inputBias(), classifier.data().outputBias());
    }

    @Override
    public Variable<Scalar> loss(Batch batch, long trainSize) {
        return crossEntropyLoss(batch);
    }

    CrossEntropyLoss crossEntropyLoss(Batch batch) {
        var batchLabels = batchLabelVector(batch);
        var batchFeatures = Objective.batchFeatureMatrix(batch, features);
        var predictions = classifier.predictionsVariable(batchFeatures);
        return new CrossEntropyLoss(
            predictions,
            batchLabels
        );
    }

    Constant<Vector> batchLabelVector(Batch batch) {
        var batchedTargets = new Vector(batch.size());
        var batchOffset = new MutableInt();

        batch.elementIds().forEach(elementId ->
            batchedTargets.setDataAt(
                batchOffset.getAndIncrement(),
                labels.get(elementId)
            )
        );

        return new Constant<>(batchedTargets);
    }



    @Override
    public MLPClassifierData modelData() {
        return classifier.data();
    }
}
