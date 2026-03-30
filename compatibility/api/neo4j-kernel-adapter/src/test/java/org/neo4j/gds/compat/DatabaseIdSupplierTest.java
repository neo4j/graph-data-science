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
package org.neo4j.gds.compat;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.SpdBuiltInProcedures;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DatabaseIdSupplierTest {

    @Test
    void shouldReadTheDatabaseNameFromDatabaseServiceAPI() {

        var contextMock = mock(Context.class);
        var graphDatabaseAPIMock = mock(GraphDatabaseAPI.class);

        when(contextMock.graphDatabaseAPI()).thenReturn(graphDatabaseAPIMock);
        when(graphDatabaseAPIMock.databaseName()).thenReturn("database-id-from-api");
        var spdProcedureProviderMock = mock(SpdBuiltInProceduresProvider.class);
        var spdBuiltInProceduresMock = mock(SpdBuiltInProcedures.class);
        when(spdProcedureProviderMock.spdBuiltInProcedures(any(Context.class))).thenReturn(spdBuiltInProceduresMock);
        when(spdBuiltInProceduresMock.isGraphShard()).thenReturn(false);

        var databaseIdSupplier = new DatabaseIdSupplier(spdProcedureProviderMock);
        var databaseName = databaseIdSupplier.databaseName(contextMock);

        assertThat(databaseName).isEqualTo("database-id-from-api");

        verify(spdBuiltInProceduresMock, times(1)).isGraphShard();
        verify(contextMock, times(1)).graphDatabaseAPI();
        verify(graphDatabaseAPIMock, times(1)).databaseName();
        verify(spdBuiltInProceduresMock, times(0)).virtualSpdName();

    }

    @Test
    void shouldReadTheDatabaseNameFromSpdBuiltInProcedures() {
        var contextMock = mock(Context.class);
        var spdProcedureProviderMock = mock(SpdBuiltInProceduresProvider.class);
        var spdBuiltInProceduresMock = mock(SpdBuiltInProcedures.class);
        when(spdProcedureProviderMock.spdBuiltInProcedures(any(Context.class))).thenReturn(spdBuiltInProceduresMock);
        when(spdBuiltInProceduresMock.isGraphShard()).thenReturn(true);
        when(spdBuiltInProceduresMock.virtualSpdName()).thenReturn("database-id-from-spd");

        var databaseIdSupplier = new DatabaseIdSupplier(spdProcedureProviderMock);
        var databaseName = databaseIdSupplier.databaseName(contextMock);

        assertThat(databaseName).isEqualTo("database-id-from-spd");
        verify(spdBuiltInProceduresMock, times(1)).isGraphShard();
        verify(spdBuiltInProceduresMock, times(1)).virtualSpdName();
        verify(contextMock, times(0)).graphDatabaseAPI();
    }

}
