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
package org.neo4j.gds.encoding;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.functions.FunctionsFacade;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OneHotEncodingFunctionTest {
    @Test
    void shouldEncode() {
        var facade = mock(GraphDataScienceProcedures.class);
        var oneHotEncodingFunction = new OneHotEncodingFunction();
        oneHotEncodingFunction.facade = facade;

        var functionsFacade = mock(FunctionsFacade.class);
        when(facade.functions()).thenReturn(functionsFacade);
        when(functionsFacade.oneHotEncoding(List.of("a", "b"), List.of("do", "re"))).thenReturn(List.of(42L, 87L));
        var encoding = oneHotEncodingFunction.oneHotEncoding(List.of("a", "b"), List.of("do", "re"));

        assertEquals(List.of(42L, 87L), encoding);
    }
}
