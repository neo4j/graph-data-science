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
import org.neo4j.gds.ml.Objective;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.L2NormSquared;
import org.neo4j.gds.ml.core.functions.LogisticLoss;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.List;

public class LinkLogisticRegressionObjective extends LinkLogisticRegressionBase implements Objective<LinkLogisticRegressionData> {
    private final double penalty;
    private final HugeObjectArray<double[]> linkFeatures;
    private final HugeDoubleArray targets;

    public LinkLogisticRegressionObjective(
        LinkLogisticRegressionData llrData,
        double penalty,
        HugeObjectArray<double[]> linkFeatures,
        HugeDoubleArray targets
    ) {
        super(llrData);
        this.penalty = penalty;
        this.linkFeatures = linkFeatures;
        this.targets = targets;
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(modelData.weights());
    }

    @Override
    public Variable<Scalar> loss(Batch relationshipBatch, long trainSize) {
        var features = features(relationshipBatch, linkFeatures);
        Variable<Matrix> predictions = predictions(features);

        var targets = makeTargetsArray(relationshipBatch);
        var penaltyVariable = new ConstantScale<>(new L2NormSquared(modelData.weights()), relationshipBatch.size() * penalty / trainSize);
        var unpenalizedLoss = new LogisticLoss(modelData.weights(), predictions, features, targets);
        return new ElementSum(List.of(unpenalizedLoss, penaltyVariable));
    }

    @Override
    public LinkLogisticRegressionData modelData() {
        return modelData;
    }

    Constant<Vector> makeTargetsArray(Batch batch) {
        var batchedTargets = new Vector(batch.size());
        var batchOffset = new MutableInt();

        batch.nodeIds().forEach(
            relationshipId -> batchedTargets.setDataAt(batchOffset.getAndIncrement(), targets.get(relationshipId))
        );

        return new Constant<>(batchedTargets);
    }
}
