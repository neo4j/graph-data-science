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
package org.neo4j.gds.ml.core.functions;

import org.ejml.data.DMatrix1Row;
import org.ejml.data.DMatrixRMaj;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.AbstractVariable;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.operations.DoubleMatrixOperations;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.IntStream;

public class LabelwiseFeatureProjection extends AbstractVariable<Matrix> {

    private final long[] nodeIds;
    private final HugeObjectArray<double[]> features;
    private final Map<NodeLabel, Weights<? extends Tensor<?>>> weightsByLabel;
    private final int projectedFeatureDimension;
    private final NodeLabel[] labels;

    public LabelwiseFeatureProjection(
        long[] nodeIds,
        HugeObjectArray<double[]> features,
        Map<NodeLabel, Weights<? extends Tensor<?>>> weightsByLabel,
        int projectedFeatureDimension,
        NodeLabel[] labels
    ) {
        super(new ArrayList<>(weightsByLabel.values()), Dimensions.matrix(nodeIds.length, projectedFeatureDimension));
        this.nodeIds = nodeIds;
        this.features = features;
        this.weightsByLabel = weightsByLabel;
        this.projectedFeatureDimension = projectedFeatureDimension;
        this.labels = labels;
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        var result = new Matrix(nodeIds.length, projectedFeatureDimension);
        IntStream.range(0, nodeIds.length).forEach(i -> {
            long nodeId = nodeIds[i];
            NodeLabel label = labels[i];
            Weights<? extends Tensor<?>> weights = weightsByLabel.get(label);
            double[] nodeFeatures = features.get(nodeId);

            DMatrix1Row wrappedWeights = DMatrixRMaj.wrap(
                weights.dimension(Dimensions.ROWS_INDEX),
                weights.dimension(Dimensions.COLUMNS_INDEX),
                weights.data().data()
            );
            var wrappedNodeFeatureVector = DMatrixRMaj.wrap(1, nodeFeatures.length, nodeFeatures);
            var productVector = new DMatrixRMaj(weights.dimension(0), 1);
            DoubleMatrixOperations.multTransB(wrappedWeights, wrappedNodeFeatureVector, productVector, index -> (index < projectedFeatureDimension));
            result.setRow(i, productVector.data);
        });
        return result;
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        var thisGradient = ctx.gradient(this);
        int rows = parent.dimension(Dimensions.ROWS_INDEX);
        int cols = parent.dimension(Dimensions.COLUMNS_INDEX);
        double[] gradientData = new double[rows * cols];

        IntStream.range(0, nodeIds.length).forEach(batchIdx -> {
            long nodeId = nodeIds[batchIdx];
            NodeLabel label = labels[batchIdx];
            Weights<? extends Tensor<?>> weights = weightsByLabel.get(label);

            if (weights == parent) {
                // perform outer product between nodeFeatures and portion thisGradient corresponding to the node
                // row is a projected feature
                // col is a non-projected feature
                double[] nodeFeatures = features.get(nodeId);
                for (int row = 0; row < rows; row++) {
                    double nodeFeatureGradient = thisGradient.dataAt(batchIdx, row);
                    for (int col = 0; col < cols; col++) {
                        gradientData[row * cols + col] += nodeFeatures[col] * nodeFeatureGradient;
                    }
                }
            }
        });
        return new Matrix(gradientData, rows, cols);
    }
}
