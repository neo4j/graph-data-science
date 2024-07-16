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
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;

class CentralityStatisticsTest {

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
           "min", "min could not be computed",
           "mean", "mean could not be computed",
           "max", "max could not be computed",
           "p50", "p50 could not be computed",
           "p75", "p75 could not be computed",
           "p90", "p90 could not be computed",
           "p95", "p95 could not be computed",
           "p99", "p99 could not be computed",
           "p999", "p999 could not be computed"
       ));
    }

    DoubleHistogram histogram(){
        var mockHistogram = mock(DoubleHistogram.class);

        var arrayIndexOutOfBoundsException= new ArrayIndexOutOfBoundsException("is out of bounds for histogram, current covered range");
        Mockito.doThrow(arrayIndexOutOfBoundsException).when(mockHistogram).recordValue(anyDouble());
        return  mockHistogram;
    }
}
