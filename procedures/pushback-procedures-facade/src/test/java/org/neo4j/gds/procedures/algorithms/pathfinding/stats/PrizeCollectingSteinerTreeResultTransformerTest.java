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
package org.neo4j.gds.procedures.algorithms.pathfinding.stats;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class PrizeCollectingSteinerTreeResultTransformerTest {

    @Mock
    private HugeLongArray parentArrayMock;
    @Mock
    private HugeDoubleArray relCostArrayMock;

    @Test
    void shouldReturnStatsResultWithCorrectValues() {
        var result = new PrizeSteinerTreeResult(
            parentArrayMock,
            relCostArrayMock,
            5,
            12.5,
            7.7
        );
        var timedResult = new TimedAlgorithmResult<>(result, 100L);
        var config = Map.<String, Object>of("param", 1);

        var transformer = new PrizeCollectingSteinerTreeStatsResultTransformer(config);

        var stream = transformer.apply(timedResult);

        assertThat(stream)
            .singleElement()
            .satisfies(stats -> {
                assertThat(stats.preProcessingMillis()).isZero();
                assertThat(stats.computeMillis()).isEqualTo(100L);
                assertThat(stats.effectiveNodeCount()).isEqualTo(5);
                assertThat(stats.totalWeight()).isEqualTo(12.5);
                assertThat(stats.sumOfPrizes()).isEqualTo(7.7);
                assertThat(stats.configuration()).isEqualTo(config);
            });
    }

    @Test
    void shouldHandleEmptyResult() {
        var timedResult = TimedAlgorithmResult.empty(PrizeSteinerTreeResult.EMPTY);

        var transformer = new PrizeCollectingSteinerTreeStatsResultTransformer(Collections.emptyMap());

        var stream = transformer.apply(timedResult);

        assertThat(stream)
            .singleElement()
            .satisfies(stats -> {
                assertThat(stats.preProcessingMillis()).isZero();
                assertThat(stats.computeMillis()).isZero();
                assertThat(stats.effectiveNodeCount()).isZero();
                assertThat(stats.totalWeight()).isZero();
                assertThat(stats.sumOfPrizes()).isZero();
                assertThat(stats.configuration()).isEmpty();
            });
    }

    @Test
    void shouldHandleNullConfiguration() {
        var result = new PrizeSteinerTreeResult(
            parentArrayMock,
            relCostArrayMock,
            2,
            3.3,
            4.4
        );
        var timedResult = new TimedAlgorithmResult<>(result, 11L);

        var transformer = new PrizeCollectingSteinerTreeStatsResultTransformer(null);

        var stream = transformer.apply(timedResult);

        assertThat(stream)
            .singleElement()
            .satisfies(stats -> {
                assertThat(stats.preProcessingMillis()).isZero();
                assertThat(stats.computeMillis()).isEqualTo(11L);
                assertThat(stats.effectiveNodeCount()).isEqualTo(2);
                assertThat(stats.totalWeight()).isEqualTo(3.3);
                assertThat(stats.sumOfPrizes()).isEqualTo(4.4);
                assertThat(stats.configuration()).isNull();
            });
    }
}
