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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.ElementWiseMax;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.MatrixSum;
import org.neo4j.gds.ml.core.functions.MatrixVectorSum;
import org.neo4j.gds.ml.core.functions.Slice;
import org.neo4j.gds.ml.core.functions.WeightedElementwiseMax;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.SubGraph;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;
import java.util.function.Function;

public class MaxPoolingAggregator implements Aggregator {

    private final Weights<Matrix> poolWeights;
    private final Weights<Matrix> selfWeights;
    private final Weights<Matrix> neighborsWeights;
    private final Weights<Vector> bias;
    private final Function<Variable<Matrix>, Variable<Matrix>> activationFunction;
    private final ActivationFunction activation;

    public MaxPoolingAggregator(
        Weights<Matrix> poolWeights,
        Weights<Matrix> selfWeights,
        Weights<Matrix> neighborsWeights,
        Weights<Vector> bias,
        ActivationFunction activationFunction
    ) {
        this.poolWeights = poolWeights;
        this.selfWeights = selfWeights;
        this.neighborsWeights = neighborsWeights;
        this.bias = bias;

        this.activationFunction = activationFunction.activationFunction();
        this.activation = activationFunction;
    }

    @Override
    public Variable<Matrix> aggregate(
        Variable<Matrix> previousLayerRepresentations,
        SubGraph subGraph
    ) {
        Variable<Matrix> weightedPreviousLayer = MatrixMultiplyWithTransposedSecondOperand.of(
            previousLayerRepresentations,
            poolWeights
        );
        Variable<Matrix> biasedWeightedPreviousLayer = new MatrixVectorSum(weightedPreviousLayer, bias);
        Variable<Matrix> neighborhoodActivations = activationFunction.apply(biasedWeightedPreviousLayer);

        Variable<Matrix> elementwiseMax = subGraph.maybeRelationshipWeightsFunction.<Variable<Matrix>>map(
            relationshipWeightsFunction ->
                // Weighted with respect to the Relationship Weights
                new WeightedElementwiseMax(neighborhoodActivations, relationshipWeightsFunction, subGraph)
        ).orElseGet(() -> new ElementWiseMax(neighborhoodActivations, subGraph.adjacency));


        Variable<Matrix> selfPreviousLayer = new Slice(previousLayerRepresentations, subGraph.mappedBatchedNodeIds);
        Variable<Matrix> self = MatrixMultiplyWithTransposedSecondOperand.of(selfPreviousLayer, selfWeights);
        Variable<Matrix> neighbors = MatrixMultiplyWithTransposedSecondOperand.of(elementwiseMax, neighborsWeights);
        Variable<Matrix> sum = new MatrixSum(List.of(self, neighbors));

        return activationFunction.apply(sum);
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(
            poolWeights,
            selfWeights,
            neighborsWeights,
            bias
        );
    }

    @Override
    public AggregatorType type() {
        return AggregatorType.POOL;
    }

    @Override
    public ActivationFunction activationFunction() {
        return activation;
    }

    public Matrix poolWeights() {
        return poolWeights.data();
    }

    public Matrix selfWeights() {
        return selfWeights.data();
    }

    public Matrix neighborsWeights() {
        return neighborsWeights.data();
    }

    public Vector bias() {
        return bias.data();
    }

}
