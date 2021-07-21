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
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

public class LinkLogisticRegressionBase {

    protected final LinkLogisticRegressionData modelData;

    LinkLogisticRegressionBase(LinkLogisticRegressionData modelData) {
        this.modelData = modelData;
    }

    protected Variable<Matrix> predictions(Constant<Matrix> features) {
        return new Sigmoid<>(MatrixMultiplyWithTransposedSecondOperand.of(features, modelData.weights()));
    }

    /**
     * @param batch Set of relationship ids
     * @param linkFeatures LinkFeatures for relationships
     */
    protected Constant<Matrix> features(Batch batch, HugeObjectArray<double[]> linkFeatures) {
        assert linkFeatures.size() > 0;

        // assume the batch contains relationship ids
        int rows = batch.size();
        int cols = linkFeatures.get(0).length;
        var batchFeatures = new Matrix(rows, cols);
        var batchFeaturesOffset = new MutableInt();

        batch.nodeIds().forEach(id -> batchFeatures.setRow(batchFeaturesOffset.getAndIncrement(), linkFeatures.get(id)));

        return new Constant<>(batchFeatures);
    }
}
