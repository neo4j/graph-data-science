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
package org.neo4j.gds.algorithms.similarity;

import org.HdrHistogram.DoubleHistogram;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;

class ActualSimilaritySummaryBuilderTest {

    @Test
    void shouldFailGracefullyOnHistogramIssues(){
        var mockHistogram = mock(DoubleHistogram.class);

        var arrayIndexOutOfBoundsException= new ArrayIndexOutOfBoundsException("is out of bounds for histogram, current covered range");
        Mockito.doThrow(arrayIndexOutOfBoundsException).when(mockHistogram).recordValue(anyDouble());

        var actualSimilaritySummaryBuilder = new ActualSimilaritySummaryBuilder(mockHistogram);

        actualSimilaritySummaryBuilder.similarityConsumer().accept( 0,0,1.0);
        actualSimilaritySummaryBuilder.similarityConsumer().accept( 0,0,1.0);
        actualSimilaritySummaryBuilder.similarityConsumer().accept( 0,0,1.0);


        var map =  actualSimilaritySummaryBuilder.similaritySummary();
        assertThat(map).isEqualTo(Map.of(
            "Error",
            "Unable to create histogram due to range of scores exceeding implementation limits."
        ));


    }


}
