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
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutParameters;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.conductance.ConductanceParameters;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.hdbscan.HDBScanParameters;
import org.neo4j.gds.hdbscan.Labels;
import org.neo4j.gds.k1coloring.K1ColoringParameters;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.kcore.KCoreDecompositionParameters;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.termination.TerminationFlag;

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
    void maxKCut() {
        var future = facade.approxMaxKCut(
            graph,
            mock(ApproxMaxKCutParameters.class),
            jobIdMock,
            true
        );
        var result = future.join();
        assertThat(result.result()).isEqualTo(ApproxMaxKCutResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void cliqueCounting(){
        var future = facade.cliqueCounting(
            graph,
            mock(CliqueCountingParameters.class),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result()).isEqualTo(CliqueCountingResult.EMPTY);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void conductance(){
        var future = facade.conductance(
            graph,
            mock(ConductanceParameters.class),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result()).isEqualTo(ConductanceResult.EMPTY);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void hdbscan(){

        var future = facade.hdbscan(
            graph,
            mock(HDBScanParameters.class),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result()).isEqualTo(Labels.EMPTY);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void k1Coloring(){

        var future = facade.k1Coloring(
            graph,
            mock(K1ColoringParameters.class),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result()).isEqualTo(K1ColoringResult.EMPTY);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void kCore(){

        var future = facade.kCore(
            graph,
            mock(KCoreDecompositionParameters.class),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result()).isEqualTo(KCoreDecompositionResult.EMPTY);
        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

}
