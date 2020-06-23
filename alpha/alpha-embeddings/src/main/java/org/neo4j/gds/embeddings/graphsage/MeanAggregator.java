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
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MultiMean;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixOpsFactory;

import java.util.List;
import java.util.function.Function;

/*
    hkv ← σ(W · MEAN({h(k−1)v } ∪ {h(k−1)u, ∀u ∈ N (v)}
*/

public class MeanAggregator implements Aggregator {

    private final Weights weights;
    private final Function<Variable, Variable> activationFunction;

    public MeanAggregator(Weights weights, Function<Variable, Variable> activationFunction) {
        this.weights = weights;
        this.activationFunction = activationFunction;
    }

    @Override
    public Variable aggregate(Variable previousLayerRepresentations, int[][] adjacencyMatrix, int[] selfAdjacency) {
        MultiMean means = new MultiMean(previousLayerRepresentations, adjacencyMatrix, selfAdjacency);
        Variable product = MatrixOpsFactory.matrixMultiply(means, weights);
        return activationFunction.apply(product);
    }

    @Override
    public List<Weights> weights() {
        return List.of(weights);
    }
}
