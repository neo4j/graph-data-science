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

import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.subgraph.BatchNeighbors;
import org.neo4j.gds.ml.core.tensor.Matrix;

import static org.neo4j.gds.ml.core.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.ml.core.Dimensions.ROWS_INDEX;


public class MultiMean extends SingleParentVariable<Matrix, Matrix> {
    private final BatchNeighbors subGraph;

    public MultiMean(
        Variable<Matrix> parentVariable,
        BatchNeighbors subGraph
    ) {
        super(parentVariable, Dimensions.matrix(subGraph.batchSize(), parentVariable.dimension(COLUMNS_INDEX)));
        this.subGraph = subGraph;

        assert parentVariable.dimension(ROWS_INDEX) >= subGraph.nodeCount() : "Expecting a row for each node in the subgraph";
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        var parentData = ctx.data(parent);

        int[] batchIds = subGraph.batchIds();
        int batchSize = batchIds.length;

        int cols = parentData.cols();

        var resultMeans = Matrix.create(0, batchSize, cols);

        for (int batchIdx = 0; batchIdx < batchSize; batchIdx++) {
            // node-ids respond to rows in parentData
            int batchNodeId = batchIds[batchIdx];
            int[] neighbors = subGraph.neighbors(batchNodeId);

            // TODO Replace this with the sum of weights instead to normalize the weights
            // degree + the node itself
            int closedNeighborHoodDegree = neighbors.length + 1;

            // initialize mean row with parent row for nodeId in batch
            for (int col = 0; col < cols; col++) {
                double sourceColEntry = parentData.dataAt(batchNodeId, col);
                resultMeans.addDataAt(batchIdx, col, sourceColEntry / closedNeighborHoodDegree);
            }

            // fetch rows from neighbors and update mean
            for (int neighbor : neighbors) {
                double relationshipWeight = subGraph.relationshipWeight(batchNodeId, neighbor);
                for (int col = 0; col < cols; col++) {
                    double neighborColData = parentData.dataAt(neighbor, col) * relationshipWeight;
                    resultMeans.addDataAt(batchIdx, col, neighborColData / closedNeighborHoodDegree);
                }
            }
        }

        // TODO try to divide by numberOfEntries once instead of on every update

        return resultMeans;
    }

    @Override
    public Matrix gradientForParent(ComputationContext ctx) {
        var multiMeanGradient = ctx.gradient(this);
        var resultGradient = ctx.data(parent).createWithSameDimensions();

        int cols = resultGradient.cols();
        var batchIds = this.subGraph.batchIds();

        for (int batchIdx = 0; batchIdx < batchIds.length; batchIdx++) {
            var batchNodeId = batchIds[batchIdx];
            int[] neighbors = subGraph.neighbors(batchNodeId);
            int closedNeighborhoodDegree = neighbors.length + 1;

            // TODO try to divide by closedNeighborhoodDegree once instead of on every update

            // pass gradient to batchNode's data
            for (int col = 0; col < cols; col++) {
                double normalizedGradient = multiMeanGradient.dataAt(batchIdx, col) / closedNeighborhoodDegree;
                resultGradient.addDataAt(batchNodeId, col, normalizedGradient);
            }

            // propagate gradient to neighbors' data
            for (int neighbor : neighbors) {
                double relationshipWeight = subGraph.relationshipWeight(batchNodeId, neighbor);

                for (int col = 0; col < cols; col++) {
                    double neighborGradient = multiMeanGradient.dataAt(batchIdx, col) * relationshipWeight;
                    resultGradient.addDataAt(neighbor * cols + col, neighborGradient / closedNeighborhoodDegree);
                }
            }
        }

        return resultGradient;
    }
}
