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
package org.neo4j.gds.applications.algorithms.pathfinding;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.allshortestpaths.MSBFSAllShortestPaths;
import org.neo4j.gds.allshortestpaths.WeightedAllShortestPaths;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MSBFSASPAlgorithmFactoryTest {

    @Test
    @DisplayName("Should create Weighted MSBFS when the algorithm parameters have relationship weight property")
    void shouldCreateWeightedMSBFS() {

        var parameters = new AllShortestPathsParameters(new Concurrency(4), true);
        var graphMock = mock(Graph.class);
        when(graphMock.hasRelationshipProperty()).thenReturn(true);

        var msbfsaspAlgorithm = MSBFSASPAlgorithmFactory.create(
            graphMock,
            parameters,
            mock(ExecutorService.class),
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        assertThat(msbfsaspAlgorithm).isInstanceOf(WeightedAllShortestPaths.class);
    }

    @Test
    @DisplayName(
        "Should fail when the algorithm parameters have relationship weight property but the graph is unweighted"
    )
    void shouldFailForUnweightedGraph() {
        var parameters = new AllShortestPathsParameters(new Concurrency(4), true);
        var graphMock = mock(Graph.class);
        when(graphMock.hasRelationshipProperty()).thenReturn(false);

        assertThatIllegalArgumentException().isThrownBy(() -> MSBFSASPAlgorithmFactory.create(
                graphMock,
                parameters,
                mock(ExecutorService.class),
                ProgressTracker.NULL_TRACKER,
                TerminationFlag.RUNNING_TRUE
            ))
            .withMessage("WeightedAllShortestPaths is not supported on graphs without a weight property");
    }


    @Test
    @DisplayName("Should create Unweighted MSBFS when the algorithm parameters don't have relationship weight property")
    void shouldCreateUnweightedMSBFS() {
        var parameters = new AllShortestPathsParameters(new Concurrency(4), false);
        var msbfsaspAlgorithm = MSBFSASPAlgorithmFactory.create(
            mock(Graph.class),
            parameters,
            mock(ExecutorService.class),
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        assertThat(msbfsaspAlgorithm).isInstanceOf(MSBFSAllShortestPaths.class);
    }


}
