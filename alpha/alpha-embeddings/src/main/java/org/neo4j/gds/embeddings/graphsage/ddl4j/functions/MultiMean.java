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
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;

public class MultiMean extends SingleParentVariable {
    private final int[][] adjacency;
    private final int[] selfAdjacency;

    public MultiMean(
        Variable parent,
        int[][] adjacency,
        int[] selfAdjacency
    ) {
        super(parent, Dimensions.matrix(adjacency.length, parent.dimension(1)));
        this.adjacency = adjacency;
        this.selfAdjacency = selfAdjacency;
    }

    @Override
    protected Tensor gradient(ComputationContext ctx) {
        double[] multiMeanGradient = ctx.gradient(this).data;
        int rows = dimension(0);
        int cols = dimension(1);

        Tensor result = ctx.data(parent).zeros();

        for (int c = 0; c < cols; c++) {
            for (int r = 0; r < rows; r++) {
                int degree = adjacency[r].length + 1;
                for (int neighbor : adjacency[r]) {
                    result.data[neighbor * cols + c] += 1d / degree * multiMeanGradient[r * cols + c];
                }
                result.data[selfAdjacency[r] * cols + c] += 1d / degree * multiMeanGradient[r * cols + c];
            }
        }


        return result;
    }

    @Override
    protected Tensor apply(ComputationContext ctx) {
        Tensor parentTensor = ctx.data(parent);
        double[] parentData = parentTensor.data;
        int parentDimensions = parent.dimension(1);
        double[] means = new double[adjacency.length * parentDimensions];
        for (int source = 0; source < adjacency.length; source++) {
            int selfAdjacencyOfSourceOffset = selfAdjacency[source] * parentDimensions;
            int sourceOffset = source * parentDimensions;
            int[] ns = adjacency[source];
            int numberOfNeighbors = ns.length;
            for (int dim = 0; dim < parentDimensions; dim++) {
                means[sourceOffset + dim] += parentData[selfAdjacencyOfSourceOffset + dim] / (numberOfNeighbors + 1);
            }
            for (int target : ns) {
                int targetOffset = target * parentDimensions;
                for (int dim = 0; dim < parentDimensions; dim++) {
                    means[sourceOffset + dim] += parentData[targetOffset + dim] / (numberOfNeighbors + 1);
                }
            }
        }

//
//        for (int source = 0; source < adj.length; source++) {
//            int sourceOffset = source * parentDimensions;
//            int numberOfNeighbors = adj[source].length;
//            for (int dim = 0; dim < parentDimensions; dim++) {
//                means[sourceOffset + dim] += parentData[sourceOffset + dim] / (numberOfNeighbors + 1);
//                for (int target = 0; target < numberOfNeighbors; target++) {
//                    int targetOffset = target * parentDimensions;
//                    means[sourceOffset + dim] += parentData[targetOffset + dim] / (numberOfNeighbors + 1);
//                }
//            }
//        }


        return new Tensor(means, dimensions());
    }
}
