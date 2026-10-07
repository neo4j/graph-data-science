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
package org.neo4j.gds.community;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.louvain.LouvainParameters;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationParameters;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.scc.SccParameters;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.wcc.WccParameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunityComputeFacadeEmptyGraphTest {
    @Mock
    private Graph graph;

    @Mock
    private ProgressTrackerFactory progressTrackerFactoryMock;

    @Mock
    private AsyncAlgorithmCaller algorithmCallerMock;

    @Mock
    private JobId jobIdMock;

    private CommunityComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(graph.isEmpty()).thenReturn(true);
        facade = new CommunityComputeFacade(
            algorithmCallerMock,
            progressTrackerFactoryMock,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Test
    void leiden(){

        var future = facade.leiden(
            graph,
            null,
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result()).isEqualTo(LeidenResult.EMPTY);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void louvain(){

        var future = facade.louvain(
            graph,
            mock(LouvainParameters.class),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result()).isEqualTo(LouvainResult.EMPTY);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void modularityOptimization(){

        var future = facade.modularityOptimization(
            graph,
            mock(ModularityOptimizationParameters.class),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result()).isEqualTo(ModularityOptimizationResult.EMPTY);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void scc(){

        var future = facade.scc(
            graph,
            mock(SccParameters.class),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().toArray()).hasSize(0);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void wcc(){

        var future = facade.wcc(
            graph,
            mock(WccParameters.class),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().size()).isEqualTo(0L);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }
}
