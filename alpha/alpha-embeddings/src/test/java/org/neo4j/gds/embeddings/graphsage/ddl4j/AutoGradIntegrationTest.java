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
package org.neo4j.gds.embeddings.graphsage.ddl4j;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Constant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixOpsFactory;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Normalise;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sum;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.TensorAdd;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;

import java.util.List;

public class AutoGradIntegrationTest implements FiniteDifferenceTest {
    @Test
    void shouldApproximateGradient() {
        Weights weights1 = new Weights(Tensor.matrix(new double[]{1, 2, 3.5, 4}, 2, 2));
        Weights weights2 = new Weights(Tensor.matrix(new double[]{6, -1.7, 2, -5}, 2, 2));
        Weights bias1 = new Weights(Tensor.vector(new double[]{3, 2.6}));
        Weights bias2 = new Weights(Tensor.vector(new double[]{-3.2, 6.2}));

        List<Weights> weights = List.of(weights1, weights2, bias1, bias2);
        Variable loss = lossFunction(weights);
        finiteDifferenceShouldApproximateGradient(weights, loss);
    }

    private Variable lossFunction(List<Weights> weights) {
        return lossFunction(weights.get(0), weights.get(1), weights.get(2), weights.get(3));
    }

    private Variable lossFunction(Weights weights1, Weights weights2, Weights bias1, Weights bias2) {
        Constant node1 = Constant.vector(new double[]{-5.2, 0});
        Constant node2 = Constant.vector(new double[]{2.3, 8.2});
        Variable node1Layer1 = new Normalise(fullyConnected(node1, weights1, bias1));
        Variable node2Layer1 = new Normalise(fullyConnected(node2, weights1, bias1));

        Variable node1Layer2 = new Normalise(fullyConnected(node1Layer1, weights2, bias2));
        Variable node2Layer2 = new Normalise(fullyConnected(node2Layer1, weights2, bias2));

        return new Sum(List.of(node1Layer2, node2Layer2));
    }

    private Variable fullyConnected(Variable features, Variable weightMatrix, Variable bias) {
        Variable prod = MatrixOpsFactory.matrixVectorMultiply(weightMatrix, features);
        Variable prodWithBias = new TensorAdd(List.of(prod, bias), prod.dimensions());
        return new Sigmoid(prodWithBias);
    }
}
