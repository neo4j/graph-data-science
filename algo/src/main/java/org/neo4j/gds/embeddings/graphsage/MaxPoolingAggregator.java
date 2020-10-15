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

import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.ElementwiseMax;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixSum;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixVectorSum;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Slice;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.WeightedElementwiseMax;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Vector;
import org.neo4j.gds.embeddings.graphsage.subgraph.SubGraph;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class MaxPoolingAggregator implements Aggregator {

    private final Optional<RelationshipWeights> maybeRelationshipWeightsFunction;
    private final Weights<Matrix> poolWeights;
    private final Weights<Matrix> selfWeights;
    private final Weights<Matrix> neighborsWeights;
    private final Weights<Vector> bias;
    private final Function<Variable<Matrix>, Variable<Matrix>> activationFunction;

    MaxPoolingAggregator(
        Optional<RelationshipWeights> maybeRelationshipWeightsFunction,
        Weights<Matrix> poolWeights,
        Weights<Matrix> selfWeights,
        Weights<Matrix> neighborsWeights,
        Weights<Vector> bias,
        Function<Variable<Matrix>, Variable<Matrix>> activationFunction
    ) {
        this.maybeRelationshipWeightsFunction = maybeRelationshipWeightsFunction;

        this.poolWeights = poolWeights;
        this.selfWeights = selfWeights;
        this.neighborsWeights = neighborsWeights;
        this.bias = bias;

        this.activationFunction = activationFunction;
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

        Variable<Matrix> elementwiseMax = maybeRelationshipWeightsFunction.<Variable<Matrix>>map(
            relationshipWeightsFunction ->
                // Weighted with respect to the Relationship Weights
                new WeightedElementwiseMax(neighborhoodActivations, relationshipWeightsFunction, subGraph)
        ).orElse(new ElementwiseMax(neighborhoodActivations, subGraph.adjacency));


        Variable<Matrix> selfPreviousLayer = new Slice(previousLayerRepresentations, subGraph.selfAdjacency);
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
}
