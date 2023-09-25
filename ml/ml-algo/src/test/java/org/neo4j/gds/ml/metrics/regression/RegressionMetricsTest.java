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
package org.neo4j.gds.ml.metrics.regression;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeDoubleArray;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RegressionMetricsTest {

    @Test
    void meanSquaredError() {
        var targets = HugeDoubleArray.of(3, -0.5, 2, 7);
        var predictions = HugeDoubleArray.of(2.5, 0, 2, 8);

        assertThat(RegressionMetrics.MEAN_SQUARED_ERROR.compute(targets, predictions)).isEqualTo(0.375);
    }

    @Test
    void rootMeanSquaredError() {
        var targets = HugeDoubleArray.of(3, -0.5, 2, 7);
        var predictions = HugeDoubleArray.of(2.5, 0, 2, 8);

        assertThat(RegressionMetrics.ROOT_MEAN_SQUARED_ERROR.compute(targets, predictions)).isEqualTo(Math.sqrt(0.375));
    }

    @Test
    void meanAbsoluteError() {
        var targets = HugeDoubleArray.of(3, -0.5, 2, 7);
        var predictions = HugeDoubleArray.of(2.5, 0, 2, 8);

        assertThat(RegressionMetrics.MEAN_ABSOLUTE_ERROR.compute(targets, predictions)).isEqualTo(0.5);
    }

    @Test
    void parseEvaluationMetrics() {
        var expectedMetrics = List.of(RegressionMetrics.MEAN_SQUARED_ERROR, RegressionMetrics.ROOT_MEAN_SQUARED_ERROR, RegressionMetrics.ROOT_MEAN_SQUARED_ERROR);
        assertThat(RegressionMetrics.parseList(List.of("MEAN_SQUARED_ERROR", "ROOT_MEAN_SQUARED_ERROR", "root_mean_squared_error"))).isEqualTo(expectedMetrics);
    }

}
