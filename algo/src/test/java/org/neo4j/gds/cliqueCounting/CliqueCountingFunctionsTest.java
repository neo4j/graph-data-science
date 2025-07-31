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
            arguments(new int[0], 0, new int[]{100,101,102,103,104,105,106}),
            arguments(new int[]{100,105}, 1, new int[]{101,102,103,104,105,106}),
            arguments(new int[]{102,104,106}, 2, new int[]{100,101,103,105,106}),
            arguments(new int[]{100,101,102,104,105,106}, 5, new int[]{103, 106})
        );
    }
    @ParameterizedTest
    @MethodSource("differenceIdsTestData")
    void shouldComputeDifferenceCorrectly(int[] exclude, int i, int[] expected){
        var include = new int[]{100,101,102,103,104,105,106};
        assertThat(CliqueCounting.difference(include, exclude, i)).containsExactly(expected);
    }


    @Test
    void shouldPartitionSubsetIdsCorrectly(){

        var subset = new long[]{10,11,12,13,14,15,16};
        var intersectionsIds = new int[7][];

        intersectionsIds[0] = new int[]{1,4};
        intersectionsIds[1] = new int[]{0,4};
        intersectionsIds[4] = new int[]{0,1};

        intersectionsIds[2] =new int[]{3,5,6};
        intersectionsIds[3] =new int[]{2,5,6};
        intersectionsIds[5] = new int[]{2,3,6};
        intersectionsIds[6] = new int[]{2,3,5};

        var subsetPartition = CliqueCounting.partitionSubsetIds(subset, intersectionsIds);
        assertThat(subsetPartition.pivotIndex()).isEqualTo(2L);
        assertThat(subsetPartition.includedNodesIds()).containsExactly(3,5,6);
        assertThat(subsetPartition.excludedNodesIds()).containsExactly(0,1,4);
    }

    static Stream<Arguments>  rootNeighbors() {
        return Stream.of(
            arguments(false,new long[]{0,1,3,4}),
            arguments(true, new long[]{3,4})
        );
    }

    @ParameterizedTest
    @MethodSource("rootNeighbors")
    void shouldFindRootNodesNeighbors(boolean filter, long[] expected){
        var inputQuery = "CREATE (a0),(a1),(a2),(a3),(a4)" +
            "(a2)-->(a0),  (a2)-->(a0),(a2)-->(a1), (a2)-->(a3),  (a2)-->(a3), (a2)-->(a3), (a2)-->(a4)";

        var graph = TestSupport.fromGdl(inputQuery, Orientation.UNDIRECTED).graph();

        var params  = mock(CliqueCountingParameters.class);
        when(params.subcliques()).thenReturn(List.of());

        var cliqueCounting = CliqueCounting.create(
            graph,
            params,
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var neighbors = cliqueCounting.rootNodeNeighbors(2,filter);
        assertThat(neighbors).containsExactly(expected);

    }
}
