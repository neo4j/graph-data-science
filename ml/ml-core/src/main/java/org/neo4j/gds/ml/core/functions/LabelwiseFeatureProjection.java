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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.collections.ha.HugeObjectArray;
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
    private final Map<NodeLabel, Weights<Matrix>> weightsByLabel;
    private final int projectedFeatureDimension;
    private final NodeLabel[] labels;

    public LabelwiseFeatureProjection(
        long[] nodeIds,
        HugeObjectArray<double[]> features,
        Map<NodeLabel, Weights<Matrix>> weightsByLabel,
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

        // TODO rewrite this into one mxm per label (with a read-only lazy feature matrix per label)
        for (int batchIdx = 0; batchIdx < nodeIds.length; batchIdx++) {
            long nodeId = nodeIds[batchIdx];
            NodeLabel label = labels[batchIdx];
            Weights<Matrix> weights = weightsByLabel.get(label);
            double[] nodeFeatures = features.get(nodeId);

            var wrappedNodeFeatureVector = new Matrix(nodeFeatures, 1, nodeFeatures.length);
            var productVector = new Matrix(weights.dimension(0), 1);
            DoubleMatrixOperations.multTransB(
                weights.data(),
                wrappedNodeFeatureVector,
                productVector,
                // TODO is filter not too simple to map the input feature dimension to the projectedDimension?
                //      we should instead assert that the nodeFeature vector is not larger
                index -> (index < projectedFeatureDimension)
            );

            result.setRow(batchIdx, productVector.data());
        }
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
