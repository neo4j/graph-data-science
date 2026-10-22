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
package org.neo4j.gds.result;

import org.HdrHistogram.DoubleHistogram;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;

class CentralityStatisticsTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldSummarizeZeroScores(int concurrency) {
        var result = CentralityStatistics.centralityStatistics(
            5, id -> 0.0, DefaultPool.INSTANCE, new Concurrency(concurrency), true
        );

        var summary = CentralityStatistics.centralitySummary(result.histogram(), result.success());
        assertThat(summary).hasSize(9);
        assertThat(summary.values()).allMatch(value -> ((Number) value).doubleValue() == 0.0);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldExcludeNaNScores(int concurrency) {
        // NaN marks excluded nodes, so only the three valid scores should contribute to the distribution.
        var scores = new double[]{Double.NaN, 0.5, 0.75, 1.0, Double.NaN};
        var result = CentralityStatistics.centralityStatistics(
            scores.length, id -> scores[(int) id], DefaultPool.INSTANCE, new Concurrency(concurrency), true
        );

        assertThat(result.success()).isTrue();
        assertThat(result.histogram().orElseThrow().getTotalCount()).isEqualTo(3);
        var summary = CentralityStatistics.centralitySummary(result.histogram(), result.success());
        assertThat((double) summary.get("min")).isCloseTo(0.5, within(1e-5));
        assertThat((double) summary.get("mean")).isCloseTo(0.75, within(1e-5));
        assertThat((double) summary.get("max")).isCloseTo(1.0, within(1e-5));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldReturnEmptyDistributionWhenAllScoresAreNaN(int concurrency) {
        // Excluding every node leaves no scores to summarize, rather than a distribution of zero scores.
        var result = CentralityStatistics.centralityStatistics(
            5, id -> Double.NaN, DefaultPool.INSTANCE, new Concurrency(concurrency), true
        );

        assertThat(result.success()).isTrue();
        assertThat(CentralityStatistics.centralitySummary(result.histogram(), result.success())).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {1,4})
    void shouldReturnFailMap(int concurrency){

       var intermediateResult= CentralityStatistics.computeCentralityStatistics(10,
            v->10.0,
            DefaultPool.INSTANCE,
            new Concurrency(concurrency),
           this::histogram,
            true
        );

       assertThat(intermediateResult.success()).isFalse();

       var map  = CentralityStatistics.centralitySummary(intermediateResult.histogram(),intermediateResult.success());
       assertThat(map).isEqualTo(Map.of(
           "Error",
           "Unable to create histogram due to range of scores exceeding implementation limits."
       ));
    }

    DoubleHistogram histogram(){
        var mockHistogram = mock(DoubleHistogram.class);

        var arrayIndexOutOfBoundsException = new ArrayIndexOutOfBoundsException("is out of bounds for histogram, current covered range");
        Mockito.doThrow(arrayIndexOutOfBoundsException).when(mockHistogram).recordValue(anyDouble());
        return  mockHistogram;
    }
}
