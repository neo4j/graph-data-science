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
package org.neo4j.gds.cliqueCounting;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CliqueCountingFunctionsTest {

    @Test
    void shouldComputeIntersectionsCorrectly(){
        var inputQuery = "CREATE (a0),(a1),(a2),(a3),(a4),(a5),(a6)" +
            "(a0)-->(a1),(a0)-->(a4),(a1)-->(a4),(a2)-->(a3),(a2)-->(a5),(a2)-->(a6),(a3)-->(a5),(a3)-->(a6),(a5)-->(a6)";

        var graph = TestSupport.fromGdl(inputQuery, Orientation.UNDIRECTED).graph();

        var subset = new long[]{0,1,2,3,4,5,6};
        var params  = mock(CliqueCountingParameters.class);
        when(params.subcliques()).thenReturn(List.of());

        var cliqueCounting = CliqueCounting.create(
            graph,
            params,
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var intersections = cliqueCounting.computeIntersections(subset);

        assertThat(intersections[0]).containsExactly(1,4);
        assertThat(intersections[1]).containsExactly(0,4);
        assertThat(intersections[4]).containsExactly(0,1);

        assertThat(intersections[2]).containsExactly(3,5,6);
        assertThat(intersections[3]).containsExactly(2,5,6);
        assertThat(intersections[5]).containsExactly(2,3,6);
        assertThat(intersections[6]).containsExactly(2,3,5);

    }



    static Stream<Arguments> differenceIdsTestData() {
        return Stream.of(
            arguments(new long[0], 103,  new int[]{0,1,2,4,5,6}),
            arguments(new long[]{100,105},  103, new int[]{1,2,4,6}),
            arguments(new long[]{105},  100, new int[]{1,2,3,4,6}),
            arguments(new long[]{105},  106, new int[]{0,1,2,3,4}),
            arguments(new long[]{100,105},  103, new int[]{1,2,4,6}),
            arguments(new long[]{100,101,102,104,105,106},  103, new int[]{})
        );
    }
    @ParameterizedTest
    @MethodSource("differenceIdsTestData")
    void shouldComputeDifferenceIdsCorrectly(long[] exclude, long pivot, int[] expected){
        var include = new long[]{100,101,102,103,104,105,106};
        assertThat(CliqueCounting.sortedDifferenceIdsWithExcludedElement(include,exclude,pivot)).containsExactly(expected);
    }

    @Test
    void shouldPartitionSubsetCorrectly(){

        var subset = new long[]{0,1,2,3,4,5,6};
        var intersections =new long[7][];

        intersections[0] = new long[]{1,4};
        intersections[1] = new long[]{0,4};
        intersections[4] = new long[]{0,1};

        intersections[2] =new long[]{3,5,6};
        intersections[3] =new long[]{2,5,6};
        intersections[5] = new long[]{2,3,6};
        intersections[6] = new long[]{2,3,5};

        var subsetPartition = CliqueCounting.partitionSubset(subset,intersections);
        assertThat(subsetPartition.pivot()).isEqualTo(2L);
        assertThat(subsetPartition.includedNodes()).containsExactly(3,5,6);
        assertThat(subsetPartition.excludedNodes()).containsExactly(0,1,4);
    }
}
