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
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithRelationshipWeights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MultiMean;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.subgraph.SubGraph;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/*
    hkv ← σ(W · MEAN({h(k−1)v } ∪ {h(k−1)u, ∀u ∈ N (v)} --> unweighted
    hkv ← σ(W · MEAN({s(u, v)^γ * h(k−1)v } ∪ {h(k−1)u, ∀u ∈ N (v)} --> weighted
*/
public class MeanAggregator implements Aggregator {

    private final Optional<RelationshipWeightsFunction> maybeRelationshipWeightsFunction;
    private final Weights<Matrix> weights;
    private final Function<Variable<Matrix>, Variable<Matrix>> activationFunction;

    MeanAggregator(Optional<RelationshipWeightsFunction> maybeRelationshipWeightsFunction, Weights<Matrix> weights, Function<Variable<Matrix>, Variable<Matrix>> activationFunction) {
        this.maybeRelationshipWeightsFunction = maybeRelationshipWeightsFunction;
        this.weights = weights;
        this.activationFunction = activationFunction;
    }

    @Override
    public Variable<Matrix> aggregate(Variable<Matrix> previousLayerRepresentations, SubGraph subGraph) {
        if (maybeRelationshipWeightsFunction.isPresent()) {
            previousLayerRepresentations = new MatrixMultiplyWithRelationshipWeights(
                previousLayerRepresentations,
                maybeRelationshipWeightsFunction.get(),
                subGraph,
                subGraph.adjacency,
                subGraph.selfAdjacency
            );
        }
        Variable<Matrix> means = new MultiMean(previousLayerRepresentations, subGraph.adjacency,
            subGraph.selfAdjacency
        );
        Variable<Matrix> product = MatrixMultiplyWithTransposedSecondOperand.of(means, weights);
        return activationFunction.apply(product);
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(weights);
    }
}
