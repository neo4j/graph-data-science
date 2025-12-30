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
package org.neo4j.gds.centrality;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.articulationPoints.ArticulationPointsParameters;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.betweenness.BetweennessCentralityParameters;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.BridgesParameters;
import org.neo4j.gds.closeness.ClosenessCentralityParameters;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.influenceMaximization.CELFParameters;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.pagerank.ArticleRankStatsConfigImpl;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.termination.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CentralityComputeFacadeEmptyGraphTest {
    @Mock
    private Graph graph;

    @Mock
    private ProgressTrackerFactory progressTrackerFactoryMock;

    @Mock
    private AsyncAlgorithmCaller algorithmCallerMock;

    @Mock
    private JobId jobIdMock;

    private CentralityComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(graph.isEmpty()).thenReturn(true);
        facade = new CentralityComputeFacade(
            algorithmCallerMock,
            progressTrackerFactoryMock,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Test
    void articleRank() {
        var config =  ArticleRankStatsConfigImpl.builder().maxIterations(3).build();

        var future = facade.articleRank(
            graph,
            config,
            jobIdMock,
            true
        );

        var result = future.join();
        assertThat(result.result()).isEqualTo(PageRankResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void articulationPoints() {

        var future = facade.articulationPoints(
            graph,
            mock(ArticulationPointsParameters.class),
            jobIdMock,
            true
        );

        var result = future.join();
        assertThat(result.result()).isEqualTo(ArticulationPointsResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void betweennessCentrality() {

        var future = facade.betweennessCentrality(
            graph,
            mock(BetweennessCentralityParameters.class),
            jobIdMock,
            true
        );

        var result = future.join();
        assertThat(result.result()).isEqualTo(BetwennessCentralityResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void bridges() {

        var future = facade.bridges(
            graph,
            mock(BridgesParameters.class),
            jobIdMock,
            true
        );

        var result = future.join();
        assertThat(result.result()).isEqualTo(BridgeResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void celf() {

        var future = facade.celf(
            graph,
            mock(CELFParameters.class),
            jobIdMock,
            true
        );

        var result = future.join();
        assertThat(result.result()).isEqualTo(CELFResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void closenessCentrality() {

        var future = facade.closeness(
            graph,
            mock(ClosenessCentralityParameters.class),
            jobIdMock,
            true
        );

        var result = future.join();
        assertThat(result.result()).isEqualTo(ClosenessCentralityResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }
}
