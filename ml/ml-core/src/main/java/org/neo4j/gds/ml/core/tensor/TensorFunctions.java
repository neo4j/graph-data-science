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
package org.neo4j.gds.ml.core.tensor;

import com.carrotsearch.hppc.predicates.DoublePredicate;

import java.util.List;

public final class TensorFunctions {

    private TensorFunctions() {}

    /**
     * Average multiple lists of tensors.
     * The operation is done in-place on the first entry of the input
     *
     * @return The averaged tensors. At position i is the average over all tensors at position i of the input lists.
     */
    public static List<? extends Tensor<? extends Tensor<?>>> averageTensors(List<? extends List<? extends Tensor<?>>> batchedTensors) {
        return averageTensors(batchedTensors, batchedTensors.size());
    }

    /**
     * Average multiple lists of tensors.
     * The operation is done in-place on the first entry of the input
     *
     * @param numberOfBatches Actual number of batches. Can be used, if the input tensors are already sums of multiple batches.
     * @return The averaged tensors. At position i is the average over all tensors at position i of the input lists.
     */
    public static List<? extends Tensor<? extends Tensor<?>>> averageTensors(List<? extends List<? extends Tensor<?>>> batchedTensors, int numberOfBatches) {
        var meanTensors = batchedTensors.get(0);

        for (int i = 0; i < meanTensors.size(); i++) {
            var currentTensor = meanTensors.get(i);
            for (int j = 1; j < batchedTensors.size(); j++) {
                Tensor<?> weightedBatchTensor = batchedTensors.get(j).get(i);
                currentTensor.addInPlace(weightedBatchTensor);
            }
            currentTensor.scalarMultiplyMutate(1D / numberOfBatches);
        }

        return meanTensors;
    }

    // copy of org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.anyMatch
    public static boolean anyMatch(double[] vector, DoublePredicate predicate) {
        boolean anyMatch = false;
        for (double v : vector) {
            if (predicate.apply(v)) {
                anyMatch = true;
                break;
            }
        }
        return anyMatch;
    }
}
