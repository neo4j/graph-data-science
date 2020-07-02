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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.ddl4j.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.ElementwiseMax;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixVectorSum;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Slice;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.TensorAdd;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;

import java.util.List;
import java.util.function.Function;

public class MaxPoolingAggregator implements Aggregator {

    private final Weights poolWeights;
    private final Weights selfWeights;
    private final Weights neighborsWeights;
    private final Weights bias;
    private final Function<Variable, Matrix> activationFunction;

    public MaxPoolingAggregator(
        Weights poolWeights,
        Weights selfWeights,
        Weights neighborsWeights,
        Weights bias,
        Function<Variable, Matrix> activationFunction) {

        this.poolWeights = poolWeights;
        this.selfWeights = selfWeights;
        this.neighborsWeights = neighborsWeights;
        this.bias = bias;

        this.activationFunction = activationFunction;
    }

    @Override
    public Matrix aggregate(
        Matrix previousLayerRepresentations,
        int[][] adjacencyMatrix,
        int[] selfAdjacencyMatrix
    ) {
        Matrix weightedPreviousLayer = MatrixMultiplyWithTransposedSecondOperand.of(
            previousLayerRepresentations,
            poolWeights
        );
        Variable biasedWeightedPreviousLayer = new MatrixVectorSum(weightedPreviousLayer, bias);
        Variable neighborhoodActivations = activationFunction.apply(biasedWeightedPreviousLayer);
        Matrix elementwiseMax = new ElementwiseMax(neighborhoodActivations, adjacencyMatrix);

        Matrix selfPreviousLayer =  new Slice(previousLayerRepresentations, selfAdjacencyMatrix);
        Variable self = MatrixMultiplyWithTransposedSecondOperand.of(selfPreviousLayer, selfWeights);
        Variable neighbors = MatrixMultiplyWithTransposedSecondOperand.of(elementwiseMax, neighborsWeights);
        Variable tensorAdd = new TensorAdd(List.of(self, neighbors), self.dimensions());

        return activationFunction.apply(tensorAdd);
    }

    @Override
    public List<Weights> weights() {
        return List.of(
            poolWeights,
            selfWeights,
            neighborsWeights,
            bias
        );
    }
}
