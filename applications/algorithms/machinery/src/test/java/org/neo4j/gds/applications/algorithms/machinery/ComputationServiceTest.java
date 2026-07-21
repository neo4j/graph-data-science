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
package org.neo4j.gds.applications.algorithms.machinery;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memory.tracking.MemoryGuardException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

class ComputationServiceTest {

    @Test
    void shouldCallMemoryExceptionParserOnMemoryGuardError() throws MemoryGuardException {
        var exception = mock(MemoryGuardException.class);
        var guard = mock(MemoryGuard.class);
        var label = mock(Label.class);

        doThrow(exception).when(guard).assertAlgorithmCanRun(
            any(),
            any(),
            anyCollection(),
            any(),
            any(),
            any(),
            any(),
            anyString(),
            any(),
            anyBoolean()
        );

        var computationService = new ComputationService(
            "foo",
            Log.noOpLog(),
            guard,
            null,
            null
        );

        var config = mock(AlgoBaseConfig.class);
        var graphResources = mock(GraphResources.class);

        try (MockedStatic<MemoryGuardExceptionParser> parser = mockStatic(MemoryGuardExceptionParser.class)) {

            try {
                computationService.computeAlgorithm(
                    config,
                    graphResources,
                    label,
                    () -> null,
                    (g, s) -> null,
                    DimensionTransformer.DISABLED
                );
            } catch (Exception e) {
                //irrelevant
            }
            parser.verify(() -> MemoryGuardExceptionParser.transformException(label, exception), times(1));
        }
    }
}
