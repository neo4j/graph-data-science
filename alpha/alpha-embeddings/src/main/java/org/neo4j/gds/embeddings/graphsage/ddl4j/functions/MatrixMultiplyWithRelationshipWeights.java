/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.subgraph.SubGraph;
import org.neo4j.gds.embeddings.graphsage.weighted.RelationshipWeightsFunction;

public class MatrixMultiplyWithRelationshipWeights extends SingleParentVariable<Matrix> {
    private final RelationshipWeightsFunction relationshipWeightsFunction;
    private final SubGraph subGraph;
    private final int[][] adjacency;
    private final int[] selfAdjacency;
    private final int rows;
    private final int cols;

    public MatrixMultiplyWithRelationshipWeights(
        Variable<Matrix> parent,
        RelationshipWeightsFunction relationshipWeightsFunction,
        SubGraph subGraph,
        int[][] adjacency,
        int[] selfAdjacency
    ) {
        super(parent, Dimensions.matrix(adjacency.length, parent.dimension(1)));
        this.relationshipWeightsFunction = relationshipWeightsFunction;
        this.subGraph = subGraph;
        this.adjacency = adjacency;
        this.selfAdjacency = selfAdjacency;
        this.rows = adjacency.length;
        this.cols = parent.dimension(1);
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        Variable<?> parent = parent();
        Tensor<?> parentTensor = ctx.data(parent);
        double[] parentData = parentTensor.data();
        double[] weightedData = new double[adjacency.length * cols];
        for (int source = 0; source < adjacency.length; source++) {
            int sourceId = selfAdjacency[source];
            long originalSourceId = subGraph.nextNodes[sourceId];
            int sourceOffset = source * cols;
            int[] neighbors = adjacency[source];
            for (int target : neighbors) {
                int targetOffset = target * cols;
                long originalTargetId = subGraph.nextNodes[target];
                double relationshipWeight = relationshipWeightsFunction.apply(originalSourceId, originalTargetId, 1.0D); //TODO normalize weights
                for (int col = 0; col < cols; col++) {
                    weightedData[sourceOffset + col] += parentData[targetOffset + col] * relationshipWeight;
                }
            }
        }

        return new Matrix(weightedData, this.rows, this.cols);
    }

    @Override
    public boolean requireGradient() {
        return true;
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        Tensor<?> result = ctx.data(parent).zeros();

        double[] weightedGradient = ctx.gradient(this).data();

        for (int col = 0; col < cols; col++) {
            for (int row = 0; row < rows; row++) {
                int sourceId = selfAdjacency[row];
                long originalSourceId = subGraph.nextNodes[sourceId];

                int gradientElementIndex = row * cols + col;
                for (int neighbor : adjacency[row]) {
                    long originalTargetId = subGraph.nextNodes[neighbor];
                    int neighborElementIndex = neighbor * cols + col;
                    double relationshipWeight = relationshipWeightsFunction.apply(originalSourceId, originalTargetId, 1.0D); //TODO normalize weights
                    double newValue = weightedGradient[gradientElementIndex] * relationshipWeight;
                    result.addDataAt(
                        neighborElementIndex,
                        newValue
                    );
                }
            }
        }

        return result;

    }
}
