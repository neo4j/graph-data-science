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
package org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.ml.Objective;
import org.neo4j.gds.ml.Trainer;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.EWiseAddMatrixScalar;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.L2NormSquared;
import org.neo4j.gds.ml.core.functions.LogisticLoss;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionTrainer;

import java.util.List;
import java.util.Optional;

public class LogisticRegressionObjective implements Objective<LogisticRegressionTrainer.LogisticRegressionData> {
    private final LogisticRegressionTrainer.LogisticRegressionData modelData;
    private final double penalty;
    private final Trainer.Features features;
    private final HugeLongArray labels;

    public LogisticRegressionObjective(
        LogisticRegressionTrainer.LogisticRegressionData llrData,
        double penalty,
        Trainer.Features features,
        HugeLongArray labels
    ) {
        this.modelData = llrData;
        this.penalty = penalty;
        this.features = features;
        this.labels = labels;

        assert features.size() > 0;
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        Optional<Weights<Scalar>> bias = modelData.bias();
        if (bias.isPresent()) {
            Weights<Scalar> weights = bias.get();
            return List.of(modelData.weights(), weights);
        } else {
            return List.of(modelData.weights());
        }
    }

    @Override
    public Variable<Scalar> loss(Batch relationshipBatch, long trainSize) {
        var batchedTargets = batchLabelVector(relationshipBatch);
        var features = batchFeatureMatrix(relationshipBatch);
        var weightedFeatures = MatrixMultiplyWithTransposedSecondOperand.of(features, modelData.weights());

        LogisticLoss unpenalizedLoss;

        Optional<Weights<Scalar>> potentialBias = modelData.bias();
        if (potentialBias.isPresent()) {
            Weights<Scalar> bias = potentialBias.get();
            var weightedFeaturesWithBias = new EWiseAddMatrixScalar(weightedFeatures, bias);
            var predictions = new Sigmoid<>(weightedFeaturesWithBias);
            unpenalizedLoss = new LogisticLoss(modelData.weights(), bias, predictions, features, batchedTargets);
        } else {
            var predictions = new Sigmoid<>(weightedFeatures);
            unpenalizedLoss = new LogisticLoss(modelData.weights(), predictions, features, batchedTargets);
        }

        var penaltyVariable = new ConstantScale<>(
            new L2NormSquared(modelData.weights()),
            relationshipBatch.size() * penalty / trainSize
        );

        return new ElementSum(List.of(unpenalizedLoss, penaltyVariable));
    }

    @Override
    public LogisticRegressionTrainer.LogisticRegressionData modelData() {
        return modelData;
    }

    private Constant<Vector> batchLabelVector(Batch batch) {
        var batchedTargets = new Vector(batch.size());
        var batchOffset = new MutableInt();

        batch.nodeIds().forEach(
            relationshipId -> batchedTargets.setDataAt(batchOffset.getAndIncrement(), labels.get(relationshipId))
        );

        return new Constant<>(batchedTargets);
    }

    private Constant<Matrix> batchFeatureMatrix(Batch batch) {
        var batchFeatures = new Matrix(batch.size(), features.get(0).length);
        var batchFeaturesOffset = new MutableInt();

        batch.nodeIds().forEach(id -> batchFeatures.setRow(batchFeaturesOffset.getAndIncrement(), features.get(id)));

        return new Constant<>(batchFeatures);
    }
}
