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
package org.neo4j.gds.metrics.procedures;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class DeprecatedProceduresMetricServiceTest {

    @Test
    void shouldRegisterCalledProcedure() {
        // given
        var registrarMock = mock(DeprecatedProceduresMetricRegistrar.class);
        var proceduresMetricService = new DeprecatedProceduresMetricService(registrarMock);

        // when
        proceduresMetricService.called("gds.alpha.deprecated.procedure");

        // then
        verify(registrarMock, times(1)).called("gds.alpha.deprecated.procedure");
        verifyNoMoreInteractions(registrarMock);
    }

}
