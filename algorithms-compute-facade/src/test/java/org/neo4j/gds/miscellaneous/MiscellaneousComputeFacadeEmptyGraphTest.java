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
package org.neo4j.gds.miscellaneous;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.ProgressTrackerFactory;
import org.neo4j.gds.scaleproperties.ScalePropertiesParameters;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.termination.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MiscellaneousComputeFacadeEmptyGraphTest {
    @Mock
    private Graph graph;

    @Mock
    private ProgressTrackerFactory progressTrackerFactoryMock;

    @Mock
    private AsyncAlgorithmCaller algorithmCallerMock;

    @Mock
    private JobId jobIdMock;

    private MiscellaneousComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(graph.isEmpty()).thenReturn(true);
        facade = new MiscellaneousComputeFacade(
            algorithmCallerMock,
            progressTrackerFactoryMock,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Test
    void scaleProperties() {
        var future = facade.scaleProperties(
            graph,
            mock(ScalePropertiesParameters.class),
            jobIdMock,
            true
        );
        var result = future.join();
        assertThat(result.result()).isEqualTo(ScalePropertiesResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }
}
