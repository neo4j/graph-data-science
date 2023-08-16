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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.procedures.GraphDataScienceProcedureFacade;
import org.neo4j.gds.procedures.catalog.CatalogFacade;
import org.neo4j.gds.procedures.catalog.GraphInfo;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GraphDropProcTest {
    @Test
    void shouldDelegateToFacade() {
        var facade = mock(GraphDataScienceProcedureFacade.class);
        var procedure = new GraphDropProc(facade);

        var expectedResult = Stream.<GraphInfo>of();
        var catalogFacade = mock(CatalogFacade.class);
        when(facade.catalog()).thenReturn(catalogFacade);
        when(catalogFacade.dropGraph("my graph", true, "some database", "some user")).thenReturn(expectedResult);
        var actualResult = procedure.dropGraph("my graph", true, "some database", "some user");

        assertSame(expectedResult, actualResult);
    }
}
