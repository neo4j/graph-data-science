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
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.catalog.CatalogProcedureFacade;
import org.neo4j.gds.projection.GraphProjectNativeResult;

import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NativeProjectProcedureTest {
    @Test
    void shouldDelegateToFacade() {
        var facade = mock(GraphDataScienceProcedures.class);
        var procedure = new GraphProjectProc(facade);

        // perhaps a bit elaborate to type these out in detail;
        // the procedure just passes through untouched whatever it gets back from the facade
        // but, I thought I'd type it out just for my own clarity
        // and same argument for this test, typing it out even though the code underneath does not much at all:
        // it confers some reassurance
        var expectedResult = Stream.of(new GraphProjectNativeResult(
            "some graph",
            Map.of("nodeProjection", Map.of(
                "label", "some label",
                "properties", emptyMap()
            )),
            Map.of("relationshipProjection", Map.of(
                "some relationship type", Map.of(
                    "type", "some relationship type",
                    "orientation", "NATURAL",
                    "aggregation", "DEFAULT",
                    "indexInverse", false,
                    "properties", emptyMap()
                )
            )),
            42,
            87,
            117
        ));
        var catalogFacade = mock(CatalogProcedureFacade.class);
        when(facade.catalog()).thenReturn(catalogFacade);
        when(catalogFacade.nativeProject(
            "some graph",
            "some label",
            "some relationship type",
            emptyMap()
        )).thenReturn(expectedResult);
        var result = procedure.project(
            "some graph",
            "some label",
            "some relationship type",
            emptyMap()
        );

        assertThat(result).isSameAs(expectedResult);
    }
}
