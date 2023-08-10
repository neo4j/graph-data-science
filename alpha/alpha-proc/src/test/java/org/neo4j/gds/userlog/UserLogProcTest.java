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
package org.neo4j.gds.userlog;


import org.junit.jupiter.api.Test;
import org.neo4j.gds.procedures.catalog.GraphStoreCatalogProcedureFacade;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.warnings.UserLogEntry;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UserLogProcTest {
    @Test
    void shouldLogUserWarnings() {
        var facade = mock(GraphStoreCatalogProcedureFacade.class);
        var userLogProc = new UserLogProc(facade);

        var expectedWarnings = Stream.of(
            new UserLogEntry(new LeafTask("lt", 42), "going once"),
            new UserLogEntry(new LeafTask("lt", 87), "going twice..."),
            new UserLogEntry(new LeafTask("lt", 23), "gone!")
        );
        when(facade.queryUserLog("unused")).thenReturn(expectedWarnings);
        var actualWarnings = userLogProc.queryUserLog("unused");

        assertThat(actualWarnings).isSameAs(expectedWarnings);
    }
}
