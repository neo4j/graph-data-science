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
import org.neo4j.gds.ml.core.RelationshipWeights;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.subgraph.SubGraph;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.graphalgo.core.utils.DoubleUtil;

public class WeightedElementwiseMax extends SingleParentVariable<Matrix> {
    private final RelationshipWeights relationshipWeights;
    private final SubGraph subGraph;
    private final int[] selfAdjacency;
    private final int[][] adjacencyMatrix;
    private final int rows;
    private final int cols;

    public WeightedElementwiseMax(
        Variable<Matrix> parent,
        RelationshipWeights relationshipWeights,
        SubGraph subGraph
    ) {
        super(parent, Dimensions.matrix(subGraph.adjacency.length, parent.dimension(1)));
        this.relationshipWeights = relationshipWeights;
        this.adjacencyMatrix = subGraph.adjacency;
        this.selfAdjacency = subGraph.selfAdjacency;
        this.subGraph = subGraph;
        this.rows = adjacencyMatrix.length;
        this.cols = parent.dimension(1);
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        Matrix max = Matrix.create(Double.NEGATIVE_INFINITY, rows, cols);

        double[] parentData = ctx.data(parent()).data();
        for (int source = 0; source < rows; source++) {
            int sourceId = selfAdjacency[source];
            long originalSourceId = subGraph.nextNodes[sourceId];
            int[] neighbors = this.adjacencyMatrix[source];
            for(int col = 0; col < cols; col++) {
                int resultElementIndex = source * cols + col;
                if (neighbors.length > 0) {
                    for (int neighbor : neighbors) {
                        long originalTargetId = subGraph.nextNodes[neighbor];
                        double relationshipWeight = relationshipWeights.weight(originalSourceId, originalTargetId);
                        int neighborElementIndex = neighbor * cols + col;
                        max.setDataAt(
                            resultElementIndex,
                            Math.max(
                                parentData[neighborElementIndex] * relationshipWeight,
                                max.dataAt(resultElementIndex)
                            )
                        );
                    }
                } else {
                    max.setDataAt(resultElementIndex, 0);
                }
            }
        }

        return max;
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        Tensor<?> result = ctx.data(parent).createWithSameDimensions();

        double[] parentData = ctx.data(parent).data();
        double[] thisGradient = ctx.gradient(this).data();
        double[] thisData = ctx.data(this).data();

        for (int source = 0; source < this.adjacencyMatrix.length; source++) {
            int sourceId = selfAdjacency[source];
            long originalSourceId = subGraph.nextNodes[sourceId];
            int[] neighbors = this.adjacencyMatrix[source];
            for (int col = 0; col < cols; col++) {
                for (int neighbor : neighbors) {
                    long originalTargetId = subGraph.nextNodes[neighbor];
                    double relationshipWeight = relationshipWeights.weight(originalSourceId, originalTargetId);

                    int thisElementIndex = source * cols + col;
                    int neighborElementIndex = neighbor * cols + col;
                    if (DoubleUtil.compareWithDefaultThreshold(parentData[neighborElementIndex] * relationshipWeight, thisData[thisElementIndex])) {
                        result.addDataAt(neighborElementIndex, thisGradient[thisElementIndex] * relationshipWeight);
                    }
                }
            }
        }

        return result;
    }
}
