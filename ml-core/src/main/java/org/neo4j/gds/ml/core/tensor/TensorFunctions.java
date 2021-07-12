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

import java.util.List;

public final class TensorFunctions {

    private TensorFunctions() {}

    // Store the result in the first batch tensors.
    public static List<? extends Tensor<? extends Tensor<?>>> averageTensors(List<? extends List<? extends Tensor<?>>> batchedTensors) {
        return averageTensors(batchedTensors, batchedTensors.size());
    }

    // Store the result in the first batch tensors.
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
}
