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
package org.neo4j.gds.ml.models.logisticregression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryRange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertMemoryRange;

class LogisticRegressionDataTest {

    @Test
    void shouldEstimateMemory() {
        var dimensions = GraphDimensions.of(0);
        var concurrency = new Concurrency(1);
        var _04_05 = LogisticRegressionData.memoryEstimation(false, 4, MemoryRange.of(5))
            .estimate(dimensions, concurrency)
            .memoryUsage();
        var _04_10 = LogisticRegressionData.memoryEstimation(false, 4, MemoryRange.of(10))
            .estimate(dimensions, concurrency)
            .memoryUsage();
        var _08_05 = LogisticRegressionData.memoryEstimation(false, 8, MemoryRange.of(5))
            .estimate(dimensions, concurrency)
            .memoryUsage();
        var _08_10 = LogisticRegressionData.memoryEstimation(false, 8, MemoryRange.of(10))
            .estimate(dimensions, concurrency)
            .memoryUsage();

        // LocalIdMap is backed by 3 arrays with each have a base estimation of 16
        var overheadForOneWeigths = 16;
        var overheadForOneBias = 16;
        var overheadForOneNLRData = overheadForOneWeigths + overheadForOneBias;

        // scaling number of classes scales memory usage linearly, modulo overhead
        assertThat(_08_05.max).isEqualTo(2 * _04_05.max - overheadForOneNLRData);
        assertThat(_08_10.max).isEqualTo(2 * _04_10.max - overheadForOneNLRData);

        var _04_20 = LogisticRegressionData.memoryEstimation(false, 4, MemoryRange.of(20))
            .estimate(dimensions, concurrency)
            .memoryUsage();
        var _04_30 = LogisticRegressionData.memoryEstimation(false, 4, MemoryRange.of(30))
            .estimate(dimensions, concurrency)
            .memoryUsage();

        // scaling number of features scales only the weights component
        // 5:   the difference in the number of features
        // * 4: the number of classes
        // * 8: size per stored value in the weights matrix
        // => the size of the change based on varying the number of features
        assertThat(_04_05.max).isEqualTo(224);
        assertThat(_04_10.max).isEqualTo(224 + 5 * 4 * 8); // five is the number of added features
        assertThat(_04_10.max).isEqualTo(_04_05.max + 5 * 4 * 8);
        assertThat(_08_10.max).isEqualTo(_08_05.max + 5 * 8 * 8);
        assertThat(_04_20.max).isEqualTo(_04_05.max + 15 * 4 * 8);
        assertThat(_04_30.max).isEqualTo(_04_05.max + 25 * 4 * 8);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "100,   0, 100, 40, 840",
        "1000,  0, 100, 40, 840",
        "100,  50, 500, 440, 4_040",
        "1000, 50, 500, 440, 4_040"
    })
    void shouldEstimateCorrectlyBinaryReduced(int relCount, int minFeatureCount, int maxFeatureCount, int minEstimation, int maxEstimation) {
        var estimatedFeatureCount = MemoryRange.of(minFeatureCount, maxFeatureCount);
        var dimensions = GraphDimensions.of(1000L, relCount);
        var memoryEstimation = LogisticRegressionData
            .memoryEstimation(true, 2, estimatedFeatureCount)
            .estimate(dimensions, new Concurrency(5000));

        assertMemoryRange(memoryEstimation.memoryUsage(), minEstimation, maxEstimation);
    }

    @Test
    void shouldCreateReducedData() {
        var data = LogisticRegressionData.withReducedClassCount(3, 3);
        var matrix = data.weights().data();

        assertThat(matrix.rows()).isEqualTo(2);
        assertThat(matrix.cols()).isEqualTo(3);
        assertThat(matrix.data()).containsExactly(new double[6]);
    }

    @Test
    void shouldCreateStandardData() {
        var data = LogisticRegressionData.standard(3, 3);
        var matrix = data.weights().data();

        assertThat(matrix.rows()).isEqualTo(3);
        assertThat(matrix.cols()).isEqualTo(3);
        assertThat(matrix.data()).containsExactly(new double[9]);
    }

}
