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
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.MultiMean;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.SubGraph;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;

import java.util.List;

/*
    hkv ← σ(W · MEAN({h(k−1)v } ∪ {h(k−1)u, ∀u ∈ N (v)} --> unweighted
    hkv ← σ(W · MEAN({s(u, v)^γ * h(k−1)v } ∪ {h(k−1)u, ∀u ∈ N (v)} --> weighted
*/
public class MeanAggregator implements Aggregator {

    private final Weights<Matrix> weights;
    private final ActivationFunction activationFunction;
    private final ActivationFunctionType activationFunctionType;

    public MeanAggregator(
        Weights<Matrix> weights,
        ActivationFunctionWrapper activationFunctionWrapper
    ) {
        this.weights = weights;
        this.activationFunction = activationFunctionWrapper.activationFunction();
        this.activationFunctionType = activationFunctionWrapper.activationFunctionType();
    }

    @Override
    public Variable<Matrix> aggregate(Variable<Matrix> previousLayerRepresentations, SubGraph subGraph) {
        Variable<Matrix> means = new MultiMean(previousLayerRepresentations, subGraph);

        Variable<Matrix> product = MatrixMultiplyWithTransposedSecondOperand.of(means, weights);
        return activationFunction.apply(product);
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(weights);
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weightsWithoutBias() {
        return List.of(weights);
    }

    @Override
    public AggregatorType type() {
        return AggregatorType.MEAN;
    }

    @Override
    public ActivationFunctionType activationFunctionType() {
        return activationFunctionType;
    }

    public Matrix weightsData() {
        return weights.data();
    }
}
