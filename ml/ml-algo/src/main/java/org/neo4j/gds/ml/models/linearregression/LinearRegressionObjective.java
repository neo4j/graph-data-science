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

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.L2NormSquared;
import org.neo4j.gds.ml.core.functions.MeanSquareError;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.gradientdescent.Objective;
import org.neo4j.gds.ml.models.Features;

import java.util.List;

public class LinearRegressionObjective implements Objective<LinearRegressionData> {

    private final Features features;
    private final HugeDoubleArray targets;
    private final LinearRegressionData modelData;
    private final double penalty;

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(modelData.weights(), modelData.bias());
    }

    LinearRegressionObjective(Features features, HugeDoubleArray targets, double penalty) {
        this.features = features;
        this.targets = targets;
        this.modelData = LinearRegressionData.of(features.featureDimension());
        this.penalty = penalty;
    }

    @Override
    public Variable<Scalar> loss(Batch batch, long trainSize) {
        LinearRegressor regressor = new LinearRegressor(modelData);
        var batchFeatures = Objective.batchFeatureMatrix(batch, features);
        var predictionsVariable = regressor.predictionsVariable(batchFeatures);
        var batchTargets = batchTargets(batch);

        return new ElementSum(List.of(
            new MeanSquareError(predictionsVariable, batchTargets),
            penaltyForBatch(batch, trainSize)
        ));
    }

    private Variable<Scalar> penaltyForBatch(Batch batch, long trainSize) {
        return new ConstantScale<>(new L2NormSquared<>(modelData().weights()), batch.size() * penalty / trainSize);
    }

    private Constant<Vector> batchTargets(Batch batch) {
        var batchedTargets = new Vector(batch.size());
        int batchOffset = 0;

        var batchIterator = batch.elementIds();

        while (batchIterator.hasNext()) {
            long elementId = batchIterator.nextLong();
            batchedTargets.setDataAt(batchOffset++, targets.get(elementId));
        }

        return new Constant<>(batchedTargets);
    }

    @Override
    public LinearRegressionData modelData() {
        return modelData;
    }

}
