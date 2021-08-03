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
package org.neo4j.gds.ml.nodemodels;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.GraphDimensions;

import static org.assertj.core.api.Assertions.assertThat;

class StatsMapTest {

    @Test
    void shouldEstimateTheSameRegardlessOfNodeCount() {
        var estimator = StatsMap.memoryEstimation(2, 5);
        var oneThousandNodes = estimator.estimate(GraphDimensions.of(1_000), 1).memoryUsage();
        var oneMillionNodes = estimator.estimate(GraphDimensions.of(1_000_000), 1).memoryUsage();
        var oneBillionNodes = estimator.estimate(GraphDimensions.of(1_000_000_000), 1).memoryUsage();
        assertThat(oneThousandNodes).isEqualTo(oneMillionNodes).isEqualTo(oneBillionNodes);
    }

    @Test
    void estimationShouldScaleWithMetricsAndParamsCounts() {
        var overheadForOneStatsMap = 40;
        var dimensions = GraphDimensions.of(1000);

        var _1_05 = StatsMap.memoryEstimation(1, 5).estimate(dimensions, 1).memoryUsage();
        var _4_05 = StatsMap.memoryEstimation(4, 5).estimate(dimensions, 1).memoryUsage();
        var _1_10 = StatsMap.memoryEstimation(1, 10).estimate(dimensions, 1).memoryUsage();
        var _4_10 = StatsMap.memoryEstimation(4, 10).estimate(dimensions, 1).memoryUsage();

        assertThat(_4_05.max).isEqualTo(4 * _1_05.max - 3 * overheadForOneStatsMap);
        assertThat(_4_10.max).isEqualTo(4 * _1_10.max - 3 * overheadForOneStatsMap);
        assertThat(_1_10.max).isEqualTo(2 * _1_05.max - overheadForOneStatsMap);
        assertThat(_4_10.max).isEqualTo(2 * _4_05.max - overheadForOneStatsMap);
    }

}
