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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NodeLogisticRegressionPredictorTest {

    @Test
    public void shouldEstimateMemoryUsage() {
        var memoryUsageInBytes = NodeLogisticRegressionPredictor.sizeOfPredictionsVariableInBytes(100, 10, 10);

        int memoryUsageOfFeatureExtractors = 320; // 32 bytes * number of features
        int memoryUsageOfFeatureMatrix = 8032; // 8 bytes * batch size * number of features + 32
        int memoryUsageOfMatrixMultiplication = 8000; // 8 bytes per double * batchSize * numberOfClasses
        int memoryUsageOfSoftMax = memoryUsageOfMatrixMultiplication; // computed over the matrix multiplication, it requires an equally-sized matrix
        assertThat(memoryUsageInBytes).isEqualTo(memoryUsageOfFeatureExtractors + memoryUsageOfFeatureMatrix + memoryUsageOfFeatureMatrix + memoryUsageOfSoftMax);
    }
}
